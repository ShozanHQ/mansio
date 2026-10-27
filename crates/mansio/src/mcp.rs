//! The MCP server: the operations as tools, over stdio.
//!
//! The transport is newline-delimited JSON-RPC 2.0 on stdin and stdout, as
//! MCP's stdio transport defines it: one message to a line, nothing else on
//! stdout. A tool has the name of its operation, `work_pull`, and takes the
//! parameters of its command as properties, `lease_id` for `--lease-id`. It
//! does what the command does, by the same code, and returns what the command
//! prints. `watch` is the one command that is not a tool: it streams, which a
//! call cannot.

use std::sync::Arc;

use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::client::Client;
use crate::ops::{self, Kind, Op, Param};

/// The protocol versions this server speaks. It answers a client with the one
/// it asked for if it is among them, and with the newest otherwise.
const VERSIONS: [&str; 4] = ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];

/// JSON-RPC: the request is not a request.
const INVALID_REQUEST: i64 = -32600;
/// JSON-RPC: no such method.
const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC: the parameters are not valid.
const INVALID_PARAMS: i64 = -32602;
/// JSON-RPC: the message is not JSON.
const PARSE_ERROR: i64 = -32700;

/// The JSON Schema of one parameter.
fn property(param: &Param) -> Value {
    match param.kind {
        Kind::Text => json!({"type": "string", "description": param.help}),
        Kind::Int => json!({"type": "integer", "minimum": 0, "description": param.help}),
        Kind::Json => json!({"description": param.help}),
        Kind::Scopes => json!({
            "type": "array",
            "description": param.help,
            "items": {
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "A topic, or facts.* or work.* for every topic of that family."
                    },
                    "verbs": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["publish", "subscribe", "claim", "ack", "approve"]}
                    }
                },
                "required": ["topic", "verbs"],
                "additionalProperties": false
            }
        }),
    }
}

/// The tool for an operation.
pub fn tool(op: Op) -> Value {
    let params = op.params();
    let properties: Map<String, Value> = params
        .iter()
        .map(|param| (param.name.to_owned(), property(param)))
        .collect();
    let required: Vec<&str> = params
        .iter()
        .filter(|param| param.required)
        .map(|param| param.name)
        .collect();
    json!({
        "name": op.name(),
        "description": op.about(),
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false
        }
    })
}

/// Every tool: the operations except those that stream.
pub fn tools() -> Vec<Value> {
    Op::ALL
        .into_iter()
        .filter(|op| op.is_tool())
        .map(tool)
        .collect()
}

fn reply(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn failure(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// What a tool call returns: text, flagged as an error or not. A tool that
/// fails is a result the model can read, not a fault of the protocol.
fn outcome(text: String, is_error: bool) -> Value {
    json!({"content": [{"type": "text", "text": text}], "isError": is_error})
}

/// Run a tool.
async fn call(client: &Client, params: &Value) -> Result<Value, (i64, String)> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or((INVALID_PARAMS, "a tool call needs a name".to_owned()))?;
    let op = Op::ALL
        .into_iter()
        .find(|op| op.is_tool() && op.name() == name)
        .ok_or_else(|| (INVALID_PARAMS, format!("there is no tool {name}")))?;
    let arguments = match params.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(arguments)) => arguments.clone(),
        Some(_) => {
            return Err((INVALID_PARAMS, "arguments must be an object".to_owned()));
        }
    };
    Ok(match ops::run(client, op, &arguments).await {
        Ok(lines) if lines.is_empty() => outcome("no output".to_owned(), false),
        Ok(lines) => outcome(
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
            false,
        ),
        Err(error) => outcome(format!("{error:#}"), true),
    })
}

/// Answer one message, or `None` if it needs no answer.
pub async fn handle(client: &Client, message: Value) -> Option<Value> {
    let Some(object) = message.as_object() else {
        return Some(failure(
            &Value::Null,
            INVALID_REQUEST,
            "a message is a JSON object",
        ));
    };
    let method = object.get("method").and_then(Value::as_str);
    let Some(id) = object.get("id").filter(|id| !id.is_null()) else {
        // A notification: nothing is answered, whatever it is.
        return None;
    };
    let Some(method) = method else {
        return Some(failure(id, INVALID_REQUEST, "a request names a method"));
    };
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    Some(match method {
        "initialize" => {
            let asked = params.get("protocolVersion").and_then(Value::as_str);
            let version = asked
                .filter(|asked| VERSIONS.contains(asked))
                .unwrap_or(VERSIONS[VERSIONS.len() - 1]);
            reply(
                id,
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "mansio", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": "Tools to publish to and read from the mansio bus, and to claim, extend, complete and fail work items. A work_pull returns an item and a lease; pass the item's itemid, the consumer and the lease_id to work_ack or work_fail. A lease ends at its expires_at: work that takes longer calls work_extend before then, again and again, and its answer's data.expires_at is the new end."
                }),
            )
        }
        "ping" => reply(id, json!({})),
        "tools/list" => reply(id, json!({"tools": tools()})),
        "tools/call" => match call(client, &params).await {
            Ok(result) => reply(id, result),
            Err((code, message)) => failure(id, code, &message),
        },
        other => failure(id, METHOD_NOT_FOUND, &format!("there is no method {other}")),
    })
}

/// What to write back for one line of input, if anything. A line that is not
/// UTF-8 or not JSON is answered with a parse error and does not end the
/// server. A batch (an array of messages, which the 2025-03-26 revision of the
/// protocol allows and later ones do not send) is answered with an array of
/// the answers, or with nothing if it held only notifications.
pub async fn answer_line(client: &Client, line: &[u8]) -> Option<String> {
    let parse_error = |why: String| {
        Some(failure(&Value::Null, PARSE_ERROR, &format!("not JSON: {why}")).to_string())
    };
    let text = match std::str::from_utf8(line) {
        Ok(text) => text,
        Err(error) => return parse_error(error.to_string()),
    };
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(batch)) if batch.is_empty() => Some(
            failure(
                &Value::Null,
                INVALID_REQUEST,
                "a batch holds a message at least",
            )
            .to_string(),
        ),
        Ok(Value::Array(batch)) => {
            let mut answers = Vec::new();
            for message in batch {
                answers.extend(handle(client, message).await);
            }
            (!answers.is_empty()).then(|| Value::Array(answers).to_string())
        }
        Ok(message) => handle(client, message)
            .await
            .map(|answer| answer.to_string()),
        Err(error) => parse_error(error.to_string()),
    }
}

/// Serve on stdin and stdout until stdin closes. Calls run side by side, so a
/// `work_pull` that waits does not hold up a `ping`.
///
/// # Errors
///
/// Fails if stdin cannot be read or stdout written.
pub async fn serve(client: Client) -> anyhow::Result<()> {
    let client = Arc::new(client);
    let (lines, mut written) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = written.recv().await {
            stdout.write_all(line.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let mut input = BufReader::new(tokio::io::stdin());
    let mut calls = JoinSet::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if input.read_until(b'\n', &mut line).await? == 0 {
            break;
        }
        let text = line.trim_ascii();
        if text.is_empty() {
            continue;
        }
        let (client, lines, text) = (Arc::clone(&client), lines.clone(), text.to_vec());
        calls.spawn(async move {
            if let Some(answer) = answer_line(&client, &text).await {
                let _ = lines.send(answer);
            }
        });
    }
    while calls.join_next().await.is_some() {}
    drop(lines);
    writer.await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Connection;

    fn client() -> Client {
        Client::from_lookup(Connection::default(), |name| {
            (name == "MANSIO_TOKEN").then(|| "t".to_owned())
        })
        .unwrap()
    }

    async fn ask(message: Value) -> Value {
        handle(&client(), message).await.unwrap()
    }

    #[tokio::test]
    async fn it_initializes_with_the_version_asked_for_when_it_knows_it() {
        for asked in VERSIONS {
            let answer = ask(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": asked, "capabilities": {}, "clientInfo": {"name": "t", "version": "1"}}
            }))
            .await;
            assert_eq!(answer["result"]["protocolVersion"], asked);
            assert_eq!(answer["id"], 1);
            assert_eq!(answer["result"]["serverInfo"]["name"], "mansio");
            assert!(answer["result"]["capabilities"]["tools"].is_object());
        }
        let answer = ask(json!({
            "jsonrpc": "2.0", "id": "a", "method": "initialize",
            "params": {"protocolVersion": "1999-01-01"}
        }))
        .await;
        assert_eq!(
            answer["result"]["protocolVersion"],
            VERSIONS[VERSIONS.len() - 1]
        );
        assert_eq!(answer["id"], "a");
    }

    #[tokio::test]
    async fn a_notification_is_not_answered_and_a_ping_is() {
        let none = handle(
            &client(),
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        )
        .await;
        assert!(none.is_none());
        let answer = ask(json!({"jsonrpc": "2.0", "id": 2, "method": "ping"})).await;
        assert_eq!(answer["result"], json!({}));
    }

    #[tokio::test]
    async fn every_operation_but_watch_is_a_tool_with_the_schema_of_its_flags() {
        let answer = ask(json!({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})).await;
        let listed = answer["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = listed
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"work_pull") && names.contains(&"webhooks_list"));
        assert!(!names.contains(&"watch"));
        assert_eq!(names.len(), Op::ALL.len() - 1);

        let pull = listed.iter().find(|t| t["name"] == "work_pull").unwrap();
        let schema = &pull["inputSchema"];
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        let mut required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        required.sort_unstable();
        assert_eq!(required, ["consumer", "queue", "tenant"]);
        assert_eq!(schema["properties"]["wait"]["type"], "integer");
        assert_eq!(schema["properties"]["tenant"]["type"], "string");
        assert!(pull["description"].as_str().unwrap().contains("lease"));

        let extend = listed.iter().find(|t| t["name"] == "work_extend").unwrap();
        let schema = &extend["inputSchema"];
        let mut required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        required.sort_unstable();
        assert_eq!(
            required,
            ["consumer", "item", "lease_id", "queue", "tenant"]
        );
        assert_eq!(schema["properties"]["seconds"]["type"], "integer");
        assert!(extend["description"].as_str().unwrap().contains("lease"));

        let create = listed
            .iter()
            .find(|t| t["name"] == "principals_create")
            .unwrap();
        assert_eq!(
            create["inputSchema"]["properties"]["scopes"]["type"],
            "array"
        );
    }

    #[tokio::test]
    async fn a_bad_call_is_a_protocol_error_or_a_tool_error_as_the_protocol_says() {
        let answer = ask(json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {"name": "no_such_tool", "arguments": {}}
        }))
        .await;
        assert_eq!(answer["error"]["code"], INVALID_PARAMS);

        let answer = ask(json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": {"name": "watch", "arguments": {}}
        }))
        .await;
        assert_eq!(
            answer["error"]["code"], INVALID_PARAMS,
            "watch is not a tool"
        );

        // A tool the model called badly answers it, so that it can try again.
        let answer = ask(json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": {"name": "work_pull", "arguments": {"tenant": "acme"}}
        }))
        .await;
        assert_eq!(answer["result"]["isError"], true);
        let text = answer["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("queue is required"), "{text}");

        let answer = ask(json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "work_pull", "arguments": "nope"}
        }))
        .await;
        assert_eq!(answer["error"]["code"], INVALID_PARAMS);
    }

    #[tokio::test]
    async fn an_unknown_method_and_a_malformed_message_are_refused() {
        let answer = ask(json!({"jsonrpc": "2.0", "id": 8, "method": "resources/list"})).await;
        assert_eq!(answer["error"]["code"], METHOD_NOT_FOUND);
        let answer = ask(json!({"jsonrpc": "2.0", "id": 9})).await;
        assert_eq!(answer["error"]["code"], INVALID_REQUEST);
        let answer = ask(json!(7)).await;
        assert_eq!(answer["error"]["code"], INVALID_REQUEST);
    }

    #[tokio::test]
    async fn a_line_that_is_not_utf8_or_not_json_is_a_parse_error_and_the_server_goes_on() {
        let client = client();
        for line in [&b"\xff\xfe{"[..], b"{", b"nonsense", b""] {
            let answer = answer_line(&client, line).await.unwrap();
            let answer: Value = serde_json::from_str(&answer).unwrap();
            assert_eq!(answer["error"]["code"], PARSE_ERROR, "{line:?}");
            assert_eq!(answer["id"], Value::Null);
        }
        let ping = answer_line(&client, br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)
            .await
            .unwrap();
        assert!(ping.contains(r#""result":{}"#), "{ping}");
    }

    #[tokio::test]
    async fn a_batch_is_answered_with_a_batch_of_the_answers() {
        let client = client();
        let line = br#"[
            {"jsonrpc":"2.0","id":1,"method":"ping"},
            {"jsonrpc":"2.0","method":"notifications/initialized"},
            {"jsonrpc":"2.0","id":2,"method":"tools/list"},
            {"jsonrpc":"2.0","id":3,"method":"nope"}
        ]"#;
        let answer = answer_line(&client, line).await.unwrap();
        let answers: Vec<Value> = serde_json::from_str(&answer).unwrap();
        let ids: Vec<&Value> = answers.iter().map(|a| &a["id"]).collect();
        assert_eq!(
            ids,
            [&json!(1), &json!(2), &json!(3)],
            "no answer to the notification"
        );
        assert!(answers[1]["result"]["tools"].is_array());
        assert_eq!(answers[2]["error"]["code"], METHOD_NOT_FOUND);

        // Only notifications: nothing is answered. An empty batch is refused.
        let quiet = br#"[{"jsonrpc":"2.0","method":"notifications/initialized"}]"#;
        assert!(answer_line(&client, quiet).await.is_none());
        let empty = answer_line(&client, b"[]").await.unwrap();
        assert!(empty.contains("-32600"), "{empty}");
    }
}
