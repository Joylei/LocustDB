use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::Arc;

use ::QueryError;
use engine::*;
use engine::query_syntax::*;
use ingest::raw_val::RawVal;
use mem_store::column::Column;
use syntax::expression::*;
use syntax::limit::*;


#[derive(Debug, Clone)]
pub struct Query {
    pub select: Vec<Expr>,
    pub table: String,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
    pub order_by: Vec<(Expr, bool)>,
    pub limit: LimitClause,
}

impl Query {
    #[inline(never)] // produces more useful profiles
    pub fn run<'a>(&self, columns: &'a HashMap<String, Arc<Column>>, explain: bool, show: bool, partition: usize)
                   -> Result<(BatchResult<'a>, Option<String>), QueryError> {
        let limit = (self.limit.limit + self.limit.offset) as usize;
        let len = columns.iter().next().unwrap().1.len();
        let mut executor = QueryExecutor::default();

        let (filter_plan, filter_type) = QueryPlan::create_query_plan(&self.filter, Filter::None, columns)?;
        let mut filter = match filter_type.encoding_type() {
            EncodingType::BitVec => {
                let mut compiled_filter = query_plan::prepare(filter_plan, &mut executor)?;
                Filter::BitVec(compiled_filter.u8()?)
            }
            _ => Filter::None,
        };

        // Sorting
        let mut sort_indices = None;
        for (plan, desc) in self.order_by.iter().rev() {
            let (plan, _) = query_plan::order_preserving(
                QueryPlan::create_query_plan(&plan, filter, columns)?);
            let ranking = query_plan::prepare(plan, &mut executor)?;

            // TODO(clemens): better criterion for using top_n
            // TODO(clemens): top_n for multiple columns?
            sort_indices = Some(if limit < len / 2 && self.order_by.len() == 1 {
                query_plan::prepare(
                    top_n(ranking, limit, *desc),
                    &mut executor)?
            } else {
                // TODO(clemens): Optimization: sort directly if only single column selected
                match sort_indices {
                    None => query_plan::prepare(
                        sort_by(ranking, indices(ranking), *desc, false /* unstable sort */),
                        &mut executor)?,
                    Some(indices) => query_plan::prepare(
                        sort_by(ranking, indices, *desc, true /* stable sort */),
                        &mut executor)?,
                }
            });
        }
        if let Some(sort_indices) = sort_indices {
            filter = Filter::Indices(sort_indices.usize()?);
        }


        let mut select = Vec::new();
        for expr in &self.select {
            let (mut plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            if let Some(codec) = plan_type.codec {
                plan = codec.decode(plan);
            }
            select.push(query_plan::prepare(plan, &mut executor)?.any());
        }
        let mut order_by = Vec::new();
        for (expr, desc) in &self.order_by {
            let (mut plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            if let Some(codec) = plan_type.codec {
                plan = codec.decode(plan);
            }
            order_by.push((query_plan::prepare(plan, &mut executor)?.any(), *desc));
        };

        for c in columns {
            debug!("{}: {:?}", partition, c);
        }
        let mut results = executor.prepare(Query::column_data(columns));
        debug!("{:#}", &executor);
        executor.run(columns.iter().next().unwrap().1.len(), &mut results, show);
        let (columns, projection, order_by) = results.collect_aliased(&select, &order_by);

        Ok(
            (BatchResult {
                columns,
                projection,
                group_by_columns: None,
                order_by,
                aggregators: Vec::with_capacity(0),
                level: 0,
                batch_count: 1,
                show,
                unsafe_referenced_buffers: results.collect_pinned(),
            },
             if explain { Some(format!("{}", executor)) } else { None }))
    }

    #[inline(never)] // produces more useful profiles
    pub fn run_aggregate<'a>(&self,
                             columns: &'a HashMap<String, Arc<Column>>,
                             explain: bool,
                             show: bool,
                             partition: usize)
                             -> Result<(BatchResult<'a>, Option<String>), QueryError> {
        trace_start!("run_aggregate");

        let mut executor = QueryExecutor::default();

        // Filter
        let (filter_plan, filter_type) = QueryPlan::create_query_plan(&self.filter, Filter::None, columns)?;
        let filter = match filter_type.encoding_type() {
            EncodingType::BitVec => {
                let compiled_filter = query_plan::prepare(filter_plan, &mut executor)?;
                Filter::BitVec(compiled_filter.u8()?)
            }
            _ => Filter::None,
        };

        // Combine all group by columns into a single decodable grouping key
        let ((grouping_key_plan, raw_grouping_key_type),
            max_grouping_key,
            decode_plans) =
            query_plan::compile_grouping_key(&self.select, filter, columns)?;
        let raw_grouping_key = query_plan::prepare(grouping_key_plan, &mut executor)?;

        // Reduce cardinality of grouping key if necessary and perform grouping
        // TODO(clemens): also determine and use is_dense. always true for hashmap, depends on group by columns for raw.
        let (encoded_group_by_column,
            grouping_key,
            grouping_key_type,
            aggregation_cardinality) =
        // TODO(clemens): refine criterion
            if max_grouping_key < 1 << 16 && raw_grouping_key_type.is_positive_integer() {
                let max_grouping_key_buf = query_plan::prepare(
                    constant(RawVal::Int(max_grouping_key), true), &mut executor)?;
                (None,
                 raw_grouping_key,
                 raw_grouping_key_type.clone(),
                 max_grouping_key_buf.const_i64())
            } else {
                query_plan::prepare_hashmap_grouping(
                    raw_grouping_key,
                    max_grouping_key as usize,
                    &mut executor)?
            };

        // Aggregators
        let mut aggregation_results = Vec::new();
        let mut selector = None;
        let mut selector_index = None;
        for (i, &(aggregator, ref expr)) in self.aggregate.iter().enumerate() {
            let (plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            let (aggregate, t) = query_plan::prepare_aggregation(
                plan,
                plan_type,
                grouping_key,
                aggregation_cardinality,
                aggregator,
                &mut executor)?;
            // TODO(clemens): if summation column is strictly positive, can use sum as well
            if aggregator == Aggregator::Count {
                selector = Some((aggregate, t.encoding_type()));
                selector_index = Some(i)
            }
            aggregation_results.push((aggregator, aggregate, t))
        }

        // Determine selector
        let selector = match selector {
            None => query_plan::prepare(
                exists(grouping_key, aggregation_cardinality.tagged()),
                &mut executor)?,
            Some(x) => x.0,
        };

        // Construct (encoded) group by column
        let encoded_group_by_column = match encoded_group_by_column {
            None => query_plan::prepare(
                nonzero_indices(selector, grouping_key_type.encoding_type()),
                &mut executor)?,
            Some(x) => x,
        };
        executor.set_encoded_group_by(encoded_group_by_column);

        // Compact and decode aggregation results
        let mut select = Vec::new();
        {
            let mut decode_compact = |aggregator: Aggregator,
                                      aggregate: TypedBufferRef,
                                      t: Type| {
                let compacted = match aggregator {
                    // TODO(clemens): if summation column is strictly positive, can use NonzeroCompact
                    Aggregator::Sum => query_plan::prepare(
                        compact(aggregate, selector),
                        &mut executor)?,
                    Aggregator::Count => query_plan::prepare(
                        nonzero_compact(aggregate),
                        &mut executor)?,
                };
                if t.is_encoded() {
                    query_plan::prepare(
                        t.codec.clone().unwrap().decode(read_buffer(compacted)),
                        &mut executor)
                } else {
                    Ok(compacted)
                }
            };

            for (i, &(aggregator, aggregate, ref t)) in aggregation_results.iter().enumerate() {
                if selector_index != Some(i) {
                    let decode_compacted = decode_compact(aggregator, aggregate, t.clone())?;
                    select.push(decode_compacted)
                }
            }

            // TODO(clemens): is there a simpler way to do this?
            if let Some(i) = selector_index {
                let (aggregator, aggregate, ref t) = aggregation_results[i];
                let selector = decode_compact(aggregator, aggregate, t.clone())?;
                select.insert(i, selector);
            }
        }

        //  Reconstruct all group by columns from grouping
        let mut grouping_columns = Vec::with_capacity(decode_plans.len());
        for (decode_plan, _t) in decode_plans {
            let decoded = query_plan::prepare_no_alias(decode_plan.clone(), &mut executor)?;
            grouping_columns.push(decoded);
        }

        // If the grouping is not order preserving, we need to sort all output columns by using the ordering constructed from the decoded group by columns
        // This is necessary to make it possible to efficiently merge with other batch results
        if !grouping_key_type.is_order_preserving() {
            let sort_indices = if raw_grouping_key_type.is_order_preserving() {
                query_plan::prepare(
                    sort_by(encoded_group_by_column,
                            indices(encoded_group_by_column),
                            false /* desc */,
                            false /* stable */),
                    &mut executor)?
            } else {
                if grouping_columns.len() != 1 {
                    bail!(QueryError::NotImplemented,
                        "Grouping key is not order preserving and more than 1 grouping column\nGrouping key type: {:?}\n{}",
                        &grouping_key_type,
                        &executor)
                }
                query_plan::prepare(
                    sort_by(grouping_columns[0],
                            indices(grouping_columns[0]),
                            false /* desc */,
                            false /* stable */),
                    &mut executor)?
            };

            let mut select2 = Vec::new();
            for s in &select {
                select2.push(query_plan::prepare_no_alias(
                    query_syntax::select(*s, sort_indices),
                    &mut executor)?);
            }
            select = select2;

            let mut grouping_columns2 = Vec::new();
            for s in &grouping_columns {
                grouping_columns2.push(query_plan::prepare_no_alias(
                    query_syntax::select(*s, sort_indices),
                    &mut executor)?);
            }
            grouping_columns = grouping_columns2;
        }

        for c in columns {
            debug!("{}: {:?}", partition, c);
        }
        let mut results = executor.prepare(Query::column_data(columns));
        debug!("{:#}", &executor);
        executor.run(columns.iter().next().unwrap().1.len(), &mut results, show);
        let group_by_cols = grouping_columns.iter().map(|i| results.collect(i.any())).collect();
        let (columns, projection, _) = results.collect_aliased(
            &select.iter().map(|s| s.any()).collect::<Vec<_>>(),
            &[]);

        let batch = BatchResult {
            columns,
            projection,
            group_by_columns: Some(group_by_cols),
            order_by: vec![],// TODO(clemens): fix
            aggregators: self.aggregate.iter().map(|x| x.0).collect(),
            level: 0,
            batch_count: 1,
            show,
            unsafe_referenced_buffers: results.collect_pinned(),
        };
        if let Err(err) = batch.validate() {
            warn!("Query result failed validation (partition {}): {}\n{:#}\nGroup By: {:?}\nSelect: {:?}",
                  partition, err, &executor, grouping_columns, select);
            Err(err)
        } else {
            Ok((
                batch,
                if explain { Some(format!("{}", executor)) } else { None }
            ))
        }
    }

    pub fn is_select_star(&self) -> bool {
        if self.select.len() == 1 {
            match self.select[0] {
                Expr::ColName(ref colname) if colname == "*" => true,
                _ => false,
            }
        } else {
            false
        }
    }

    pub fn result_column_names(&self) -> Vec<String> {
        let mut anon_columns = -1;
        let select_cols = self.select
            .iter()
            .map(|expr| match *expr {
                Expr::ColName(ref name) => name.clone(),
                _ => {
                    anon_columns += 1;
                    format!("col_{}", anon_columns)
                }
            });
        let mut anon_aggregates = -1;
        let aggregate_cols = self.aggregate
            .iter()
            .map(|&(agg, _)| {
                anon_aggregates += 1;
                match agg {
                    Aggregator::Count => format!("count_{}", anon_aggregates),
                    Aggregator::Sum => format!("sum_{}", anon_aggregates),
                }
            });

        select_cols.chain(aggregate_cols).collect()
    }

    pub fn find_referenced_cols(&self) -> HashSet<String> {
        let mut colnames = HashSet::new();
        for expr in &self.select {
            expr.add_colnames(&mut colnames);
        }
        for expr in &self.order_by {
            expr.0.add_colnames(&mut colnames);
        }
        self.filter.add_colnames(&mut colnames);
        for &(_, ref expr) in &self.aggregate {
            expr.add_colnames(&mut colnames);
        }
        colnames
    }

    fn column_data(columns: &HashMap<String, Arc<Column>>) -> HashMap<String, Vec<&AnyVec>> {
        columns.iter()
            .map(|(name, column)| (name.to_string(), column.data_sections()))
            .collect()
    }
}


