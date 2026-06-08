use crate::types::{
    AggregateStrategy, DataValue, ExportFormat, FilterExpr, Op, PipelineStage, Query,
};
use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, digit1, multispace0},
    combinator::{map, opt, value},
    multi::separated_list0,
    sequence::{delimited, preceded, tuple},
};
use ordered_float::OrderedFloat;

/// Parses a full query pipeline into a vector of stages.
/// Example input: `from("logs") | filter(status == "error") | limit(10)`
pub fn parse_pipeline(input: &str) -> Result<Vec<PipelineStage>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    // Split by pipe '|' and parse each stage
    let mut stages = Vec::new();
    let parts: Vec<&str> = trimmed.split('|').collect();

    for part in parts {
        let part_trimmed = part.trim();
        if part_trimmed.is_empty() {
            return Err("Empty pipeline stage or trailing/consecutive pipe '|'".to_string());
        }
        match parse_stage(part_trimmed) {
            Ok((remaining, stage)) => {
                let rem_trimmed = remaining.trim();
                if !rem_trimmed.is_empty() {
                    return Err(format!(
                        "Unexpected trailing characters '{}' in stage '{}'",
                        rem_trimmed, part_trimmed
                    ));
                }
                stages.push(stage);
            }
            Err(e) => return Err(format!("Parse error in stage '{}': {:?}", part_trimmed, e)),
        }
    }

    Ok(stages)
}

fn parse_stage(input: &str) -> IResult<&str, PipelineStage> {
    alt((
        parse_from_stage,
        parse_delete_stage,
        parse_trash_stage,
        parse_filter_stage,
        parse_vector_filter_stage,
        parse_map_stage,
        parse_window_stage,
        parse_limit_stage,
        parse_export_stage,
        parse_enrich_stage,
        parse_get_stage,
        parse_count_stage,
        parse_sort_stage,
        parse_page_stage,
        parse_group_stage,
    ))(input)
}

fn parse_get_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("get")(input)?;
    let (input, key) = delimited(char('('), ws(parse_string_or_ident), char(')'))(input)?;
    Ok((input, PipelineStage::Get { key }))
}

fn parse_count_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("count")(input)?;
    let (input, _) = opt(delimited(char('('), multispace0, char(')')))(input)?;
    Ok((input, PipelineStage::Count))
}

fn parse_sort_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("sort")(input)?;
    let (input, (field, dir)) = delimited(
        char('('),
        ws(tuple((
            parse_identifier,
            opt(preceded(ws(char(',')), ws(alt((tag("asc"), tag("desc")))))),
        ))),
        char(')'),
    )(input)?;
    let descending = match dir {
        Some("desc") => true,
        _ => false,
    };
    Ok((input, PipelineStage::Sort { field, descending }))
}

fn parse_page_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("page")(input)?;
    let (input, stage) = delimited(
        char('('),
        alt((
            map(
                tuple((
                    tag("cursor"),
                    delimited(char('('), ws(parse_string_literal), char(')')),
                    ws(char(',')),
                    ws(parse_digits),
                )),
                |(_, cursor, _, size_str)| {
                    let page_size = size_str.parse::<usize>().unwrap_or(50);
                    PipelineStage::PageCursor { cursor, page_size }
                },
            ),
            map(
                tuple((ws(parse_digits), ws(char(',')), ws(parse_digits))),
                |(num_str, _, size_str)| {
                    let page_number = num_str.parse::<usize>().unwrap_or(1);
                    let page_size = size_str.parse::<usize>().unwrap_or(50);
                    PipelineStage::Page {
                        page_number,
                        page_size,
                    }
                },
            ),
        )),
        char(')'),
    )(input)?;
    Ok((input, stage))
}

fn parse_aggregation_func(input: &str) -> IResult<&str, String> {
    alt((
        map(tuple((parse_identifier, tag("()"))), |(name, _)| {
            format!("{}()", name)
        }),
        map(
            tuple((parse_identifier, char('('), parse_identifier, char(')'))),
            |(name, _, arg, _)| format!("{}({})", name, arg),
        ),
    ))(input)
}

fn parse_group_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("group")(input)?;
    let (input, (field, _, aggregations)) = delimited(
        char('('),
        ws(tuple((
            parse_identifier,
            ws(char(',')),
            separated_list0(ws(char(',')), parse_aggregation_func),
        ))),
        char(')'),
    )(input)?;
    Ok((
        input,
        PipelineStage::Group {
            field,
            aggregations,
        },
    ))
}

// Help parsers
fn ws<I, O, E, F>(inner: F) -> impl FnMut(I) -> IResult<I, O, E>
where
    I: nom::InputTakeAtPosition,
    <I as nom::InputTakeAtPosition>::Item: nom::AsChar + Clone,
    E: nom::error::ParseError<I>,
    F: nom::Parser<I, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

fn parse_digits(input: &str) -> IResult<&str, &str> {
    digit1(input)
}

fn parse_tag_or(input: &str) -> IResult<&str, &str> {
    tag("or")(input)
}

fn parse_tag_and(input: &str) -> IResult<&str, &str> {
    tag("and")(input)
}

fn parse_string_literal(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"')(input)?;
    let (input, content) = take_while(|c| c != '"')(input)?;
    let (input, _) = char('"')(input)?;
    Ok((input, content.to_string()))
}

fn parse_identifier(input: &str) -> IResult<&str, String> {
    let (input, head) = take_while1(|c: char| c.is_alphabetic() || c == '_')(input)?;
    let (input, tail) = take_while(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    Ok((input, format!("{}{}", head, tail)))
}

fn parse_string_or_ident(input: &str) -> IResult<&str, String> {
    alt((parse_string_literal, parse_identifier))(input)
}

// Parses `from("stream_name")` or `from(stream_name)`
fn parse_from_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("from")(input)?;
    let (input, name) = delimited(char('('), ws(parse_string_or_ident), char(')'))(input)?;
    Ok((input, PipelineStage::From { stream_name: name }))
}

fn parse_delete_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("delete")(input)?;
    let (input, _) = opt(delimited(char('('), multispace0, char(')')))(input)?;
    Ok((input, PipelineStage::Delete))
}

fn parse_trash_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("trash")(input)?;
    let (input, _) = opt(delimited(char('('), multispace0, char(')')))(input)?;
    Ok((input, PipelineStage::Trash))
}

fn parse_filter_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("filter")(input)?;
    let (input, expr) = delimited(char('('), ws(parse_filter_expr), char(')'))(input)?;
    Ok((input, PipelineStage::Filter { expr }))
}

fn parse_vector_filter_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("vector_filter")(input)?;
    let (input, _) = char('(')(input)?;
    let (input, field) = ws(parse_identifier)(input)?;
    let (input, _) = char(',')(input)?;
    let (input, query_val) = ws(parse_data_value)(input)?;
    let (input, _) = char(',')(input)?;
    let (input, threshold_val) = ws(parse_data_value)(input)?;
    let (input, _) = char(')')(input)?;

    let query_vector = match crate::executor::to_vector(&query_val) {
        Some(v) => v,
        None => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    let threshold = match threshold_val {
        DataValue::Float(f) => f,
        DataValue::Int(i) => OrderedFloat(i as f64),
        DataValue::UInt(u) => OrderedFloat(u as f64),
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    Ok((
        input,
        PipelineStage::VectorFilter {
            field,
            query_vector,
            threshold,
        },
    ))
}

fn parse_filter_expr(input: &str) -> IResult<&str, FilterExpr> {
    parse_or_expr(input)
}

fn parse_or_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (mut input, mut left) = parse_and_expr(input)?;
    while let Ok((next_input, _)) = ws(parse_tag_or)(input) {
        let (next_input, right) = parse_and_expr(next_input)?;
        left = FilterExpr::Or {
            left: Box::new(left),
            right: Box::new(right),
        };
        input = next_input;
    }
    Ok((input, left))
}

fn parse_and_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (mut input, mut left) = parse_primary_expr(input)?;
    while let Ok((next_input, _)) = ws(parse_tag_and)(input) {
        let (next_input, right) = parse_primary_expr(next_input)?;
        left = FilterExpr::And {
            left: Box::new(left),
            right: Box::new(right),
        };
        input = next_input;
    }
    Ok((input, left))
}

fn parse_primary_expr(input: &str) -> IResult<&str, FilterExpr> {
    alt((
        delimited(char('('), ws(parse_filter_expr), char(')')),
        parse_simple_filter_expr,
    ))(input)
}

fn parse_simple_filter_expr(input: &str) -> IResult<&str, FilterExpr> {
    let (input, (field, op, val)) =
        tuple((ws(parse_identifier), ws(parse_op), ws(parse_data_value)))(input)?;
    Ok((
        input,
        FilterExpr::Simple {
            field,
            operator: op,
            value: val,
        },
    ))
}

fn parse_op(input: &str) -> IResult<&str, Op> {
    alt((
        value(Op::Eq, tag("==")),
        value(Op::NotEq, tag("!=")),
        value(Op::GtEq, tag(">=")),
        value(Op::LtEq, tag("<=")),
        value(Op::Gt, tag(">")),
        value(Op::Lt, tag("<")),
        value(Op::In, tag("in")),
        value(Op::StartsWith, tag("startsWith")),
    ))(input)
}

fn parse_array_literal(input: &str) -> IResult<&str, DataValue> {
    let (input, elements) = delimited(
        char('['),
        ws(separated_list0(ws(char(',')), parse_data_value)),
        char(']'),
    )(input)?;
    Ok((input, DataValue::Array(elements)))
}

fn parse_data_value(input: &str) -> IResult<&str, DataValue> {
    alt((
        map(parse_string_literal, DataValue::String),
        value(DataValue::Null, tag("null")),
        value(DataValue::Bool(true), tag("true")),
        value(DataValue::Bool(false), tag("false")),
        parse_number,
        parse_array_literal,
    ))(input)
}

fn parse_number(input: &str) -> IResult<&str, DataValue> {
    let (input, sign) = opt(char('-'))(input)?;
    let (input, whole) = digit1(input)?;
    let (input, decimal) = opt(preceded(char('.'), digit1))(input)?;

    let is_negative = sign.is_some();

    if let Some(dec) = decimal {
        let val_str = format!("{}{}.{}", if is_negative { "-" } else { "" }, whole, dec);
        let val: f64 = val_str.parse().unwrap();
        Ok((input, DataValue::Float(OrderedFloat(val))))
    } else {
        let val_str = format!("{}{}", if is_negative { "-" } else { "" }, whole);
        if is_negative {
            let val: i64 = val_str.parse().unwrap();
            Ok((input, DataValue::Int(val)))
        } else if let Ok(val) = val_str.parse::<u64>() {
            Ok((input, DataValue::UInt(val)))
        } else {
            let val: i64 = val_str.parse().unwrap();
            Ok((input, DataValue::Int(val)))
        }
    }
}

// Parses `map(field1, field2, ...)`
fn parse_map_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("map")(input)?;
    let (input, projections) = delimited(
        char('('),
        ws(separated_list0(ws(char(',')), parse_string_or_ident)),
        char(')'),
    )(input)?;
    Ok((input, PipelineStage::Map { projections }))
}

// Parses `window(10000, average)` or `window(5000, count)`
fn parse_window_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("window")(input)?;
    let (input, (duration_str, _, strategy_str)) = delimited(
        char('('),
        ws(tuple((digit1, ws(char(',')), parse_identifier))),
        char(')'),
    )(input)?;

    let duration_ms = duration_str.parse::<u64>().unwrap();
    let strategy = match strategy_str.to_lowercase().as_str() {
        "count" => AggregateStrategy::Count,
        "sum" => AggregateStrategy::Sum,
        "average" | "avg" => AggregateStrategy::Average,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    Ok((
        input,
        PipelineStage::Window {
            duration_ms,
            strategy,
        },
    ))
}

// Parses `limit(10)`
fn parse_limit_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("limit")(input)?;
    let (input, count_str) = delimited(char('('), ws(digit1), char(')'))(input)?;
    let count = count_str.parse::<usize>().unwrap();
    Ok((input, PipelineStage::Limit { count }))
}

// Parses `export(jsonl)` or `export("csv")`
fn parse_export_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("export")(input)?;
    let (input, format_str) = delimited(char('('), ws(parse_string_or_ident), char(')'))(input)?;
    let format = match format_str.to_lowercase().as_str() {
        "jsonl" => ExportFormat::Jsonl,
        "csv" => ExportFormat::Csv,
        "msgpack" | "messagepack" => ExportFormat::MsgPack,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };
    Ok((input, PipelineStage::Export { format }))
}

// Parses `enrich(customers, customer_id)` or `enrich("customers", "customer_id")`
fn parse_enrich_stage(input: &str) -> IResult<&str, PipelineStage> {
    let (input, _) = tag("enrich")(input)?;
    let (input, (source_stream, _, join_key)) = delimited(
        char('('),
        ws(tuple((
            parse_string_or_ident,
            ws(char(',')),
            parse_string_or_ident,
        ))),
        char(')'),
    )(input)?;

    Ok((
        input,
        PipelineStage::Enrich {
            source_stream,
            join_key,
        },
    ))
}

fn find_outer_comma(input: &str) -> Option<usize> {
    let mut bracket_depth = 0;
    let mut paren_depth = 0;
    let mut brace_depth = 0;
    let mut in_quotes = false;
    for (i, c) in input.char_indices() {
        if c == '"' {
            in_quotes = !in_quotes;
        } else if !in_quotes {
            match c {
                '[' => bracket_depth += 1,
                ']' => bracket_depth -= 1,
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                '{' => brace_depth += 1,
                '}' => brace_depth -= 1,
                ',' => {
                    if bracket_depth == 0 && paren_depth == 0 && brace_depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn parse_js_value(input: &str) -> IResult<&str, serde_json::Value> {
    alt((
        map(parse_string_literal, serde_json::Value::String),
        map(parse_number, |dv| match dv {
            DataValue::Int(i) => serde_json::Value::Number(i.into()),
            DataValue::UInt(u) => serde_json::Value::Number(u.into()),
            DataValue::Float(f) => serde_json::Number::from_f64(f.into_inner())
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        }),
        value(serde_json::Value::Bool(true), tag("true")),
        value(serde_json::Value::Bool(false), tag("false")),
        value(serde_json::Value::Null, tag("null")),
        map(
            take_while1(|c| c != ',' && c != '}' && c != ']'),
            |s: &str| {
                let s_trimmed = s.trim();
                if s_trimmed == "true" {
                    serde_json::Value::Bool(true)
                } else if s_trimmed == "false" {
                    serde_json::Value::Bool(false)
                } else if s_trimmed == "null" {
                    serde_json::Value::Null
                } else if let Ok(i) = s_trimmed.parse::<i64>() {
                    serde_json::Value::Number(i.into())
                } else if let Ok(f) = s_trimmed.parse::<f64>() {
                    serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::String(s_trimmed.to_string())
                }
            },
        ),
    ))(input)
}

fn parse_js_array(input: &str) -> IResult<&str, serde_json::Value> {
    let (input, elements) = delimited(
        char('['),
        ws(separated_list0(ws(char(',')), parse_js_any_val)),
        char(']'),
    )(input)?;
    Ok((input, serde_json::Value::Array(elements)))
}

fn parse_js_object(input: &str) -> IResult<&str, serde_json::Value> {
    let (input, pairs) = delimited(
        char('{'),
        ws(separated_list0(
            ws(char(',')),
            tuple((
                ws(parse_string_or_ident),
                ws(char(':')),
                ws(parse_js_any_val),
            )),
        )),
        char('}'),
    )(input)?;
    let mut map = serde_json::Map::new();
    for (k, _, v) in pairs {
        map.insert(k, v);
    }
    Ok((input, serde_json::Value::Object(map)))
}

fn parse_js_any_val(input: &str) -> IResult<&str, serde_json::Value> {
    alt((parse_js_object, parse_js_array, parse_js_value))(input)
}

pub fn parse_query(input: &str) -> Result<Query, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("Empty query".to_string());
    }

    if trimmed == "streams"
        || trimmed == "streams()"
        || (trimmed.starts_with("streams(")
            && trimmed.ends_with(")")
            && trimmed["streams(".len()..trimmed.len() - 1]
                .trim()
                .is_empty())
    {
        return Ok(Query::ListStreams);
    }

    if trimmed == "status"
        || trimmed == "status()"
        || (trimmed.starts_with("status(")
            && trimmed.ends_with(")")
            && trimmed["status(".len()..trimmed.len() - 1]
                .trim()
                .is_empty())
    {
        return Ok(Query::Status);
    }

    if trimmed.starts_with("drop") {
        if let Ok((remaining, stream_name)) = delimited(
            tuple((tag("drop"), char('('))),
            ws(parse_string_or_ident),
            char(')'),
        )(trimmed)
        {
            if remaining.trim().is_empty() {
                return Ok(Query::Drop { stream_name });
            } else {
                return Err(format!(
                    "Unexpected trailing characters after drop: '{}'",
                    remaining.trim()
                ));
            }
        }
    }

    let mut dot_index = None;
    let mut bracket_depth = 0;
    let mut paren_depth = 0;
    let mut brace_depth = 0;
    let mut in_quotes = false;
    let chars: Vec<char> = trimmed.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '"' {
            in_quotes = !in_quotes;
        } else if !in_quotes {
            match c {
                '[' => bracket_depth += 1,
                ']' => bracket_depth -= 1,
                '(' => paren_depth += 1,
                ')' => paren_depth -= 1,
                '{' => brace_depth += 1,
                '}' => brace_depth -= 1,
                '.' => {
                    if bracket_depth == 0 && paren_depth == 0 && brace_depth == 0 {
                        let rest: String = chars[i..].iter().collect();
                        if rest.starts_with(".update")
                            || rest.starts_with(".delete")
                            || rest.starts_with(".empty")
                            || rest.starts_with(".insert")
                            || rest.starts_with(".upsert")
                        {
                            dot_index = Some(i);
                        }
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }

    if let Some(idx) = dot_index {
        let pipeline_part = trimmed[..idx].trim();
        let action_part = trimmed[idx..].trim();

        let pipeline = parse_pipeline(pipeline_part)?;

        if action_part.starts_with(".delete") {
            let arg_str = action_part.strip_prefix(".delete").unwrap().trim();
            if arg_str.starts_with('(') && arg_str.ends_with(')') {
                let inner = arg_str[1..arg_str.len() - 1].trim();
                if !inner.is_empty() {
                    if pipeline.len() == 1 {
                        if let PipelineStage::From { stream_name } = &pipeline[0] {
                            let key = inner.trim_matches('"').to_string();
                            return Ok(Query::DeleteKey {
                                stream_name: stream_name.clone(),
                                key,
                            });
                        }
                    }
                }
            }
            return Ok(Query::PipelineDelete { pipeline });
        } else if action_part.starts_with(".empty") {
            if pipeline.len() == 1 {
                if let PipelineStage::From { stream_name } = &pipeline[0] {
                    return Ok(Query::Empty {
                        stream_name: stream_name.clone(),
                    });
                }
            }
            return Err("Empty action must be applied directly to from(stream)".to_string());
        } else if action_part.starts_with(".insert") {
            if pipeline.len() == 1 {
                if let PipelineStage::From { stream_name } = &pipeline[0] {
                    let arg_str = action_part.strip_prefix(".insert").unwrap().trim();
                    if arg_str.starts_with('(') && arg_str.ends_with(')') {
                        let inner = arg_str[1..arg_str.len() - 1].trim();
                        if inner.starts_with('[') {
                            if let Ok((_, batch_val)) = parse_js_array(inner) {
                                if let serde_json::Value::Array(arr) = batch_val {
                                    let mut batch = Vec::new();
                                    for item in arr {
                                        if let serde_json::Value::Array(pair) = item {
                                            if pair.len() == 2 {
                                                let key = match &pair[0] {
                                                    serde_json::Value::String(s) => s.clone(),
                                                    other => other.to_string(),
                                                };
                                                batch.push((key, pair[1].clone()));
                                            }
                                        }
                                    }
                                    return Ok(Query::InsertBatch {
                                        stream_name: stream_name.clone(),
                                        batch,
                                    });
                                }
                            }
                        } else {
                            if let Some(comma_idx) = find_outer_comma(inner) {
                                let key_part =
                                    inner[..comma_idx].trim().trim_matches('"').to_string();
                                let val_part = inner[comma_idx + 1..].trim();
                                if let Ok((_, value)) = parse_js_any_val(val_part) {
                                    return Ok(Query::Insert {
                                        stream_name: stream_name.clone(),
                                        key: key_part,
                                        value,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            return Err("Insert action must be applied directly to from(stream)".to_string());
        } else if action_part.starts_with(".upsert") {
            if pipeline.len() == 1 {
                if let PipelineStage::From { stream_name } = &pipeline[0] {
                    let arg_str = action_part.strip_prefix(".upsert").unwrap().trim();
                    if arg_str.starts_with('(') && arg_str.ends_with(')') {
                        let inner = arg_str[1..arg_str.len() - 1].trim();
                        if inner.starts_with('[') {
                            if let Ok((_, batch_val)) = parse_js_array(inner) {
                                if let serde_json::Value::Array(arr) = batch_val {
                                    let mut batch = Vec::new();
                                    for item in arr {
                                        if let serde_json::Value::Array(pair) = item {
                                            if pair.len() == 2 {
                                                let key = match &pair[0] {
                                                    serde_json::Value::String(s) => s.clone(),
                                                    other => other.to_string(),
                                                };
                                                batch.push((key, pair[1].clone()));
                                            }
                                        }
                                    }
                                    return Ok(Query::UpsertBatch {
                                        stream_name: stream_name.clone(),
                                        batch,
                                    });
                                }
                            }
                        } else {
                            if let Some(comma_idx) = find_outer_comma(inner) {
                                let key_part =
                                    inner[..comma_idx].trim().trim_matches('"').to_string();
                                let val_part = inner[comma_idx + 1..].trim();
                                if let Ok((_, value)) = parse_js_any_val(val_part) {
                                    return Ok(Query::Upsert {
                                        stream_name: stream_name.clone(),
                                        key: key_part,
                                        value,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            return Err("Upsert action must be applied directly to from(stream)".to_string());
        } else if action_part.starts_with(".update") {
            let arg_str = action_part.strip_prefix(".update").unwrap().trim();
            if arg_str.starts_with('(') && arg_str.ends_with(')') {
                let inner = arg_str[1..arg_str.len() - 1].trim();
                if pipeline.len() == 1 {
                    if let PipelineStage::From { stream_name } = &pipeline[0] {
                        if let Some(comma_idx) = find_outer_comma(inner) {
                            let key_part = inner[..comma_idx].trim().trim_matches('"').to_string();
                            let val_part = inner[comma_idx + 1..].trim();
                            if let Ok((_, value)) = parse_js_any_val(val_part) {
                                return Ok(Query::Update {
                                    stream_name: stream_name.clone(),
                                    key: key_part,
                                    value,
                                });
                            }
                        }
                    }
                }
                if let Ok((_, value)) = parse_js_any_val(inner) {
                    return Ok(Query::PipelineUpdate {
                        pipeline,
                        update_value: value,
                    });
                }
            }
        }
    }

    let pipeline = parse_pipeline(trimmed)?;
    Ok(Query::Pipeline(pipeline))
}
