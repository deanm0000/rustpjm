use crate::errors::*;
use crate::structs::cust::*;
use axum::http::HeaderMap;
use azure_storage::prelude::*;
use azure_storage_queues::prelude::*;
use azure_storage_queues::QueueClient;
use base64::prelude::*;
use core::time::Duration;
use object_store::azure::MicrosoftAzureBuilder;
use reqwest::{header::HeaderValue, Client};
use serde_json::to_string;
use std::{borrow::Cow, env};

pub async fn put_to_queue(msg: &InMsg, visibility_timeout_secs: u32) -> Result<(), Errors> {
    let queue_name = msg.pjm_end_point.url_suffix.replace("_", "");
    let queue_client = make_queue_client(queue_name.as_str());
    let msg_json: InMsgJson = msg.into();
    let new_queue_json = match to_string(&msg_json) {
        Ok(new_queue_json) => new_queue_json,
        _ => return Err(Errors::QTToString),
    };
    let new_queue_b64 = BASE64_STANDARD.encode(new_queue_json);

    let message = {
        let message = queue_client.put_message(new_queue_b64);
        if visibility_timeout_secs > 0 {
            message.visibility_timeout(Duration::new(visibility_timeout_secs as u64, 0))
        } else {
            message
        }
    };

    let msg_resp = message.into_future().await;
    if let Err(e) = msg_resp {
        eprintln!(
            "{:?} {} queue error {}",
            msg.begin_time, msg.pjm_end_point.url_suffix, e
        );
        if let Ok(poo) = queue_client.url() {
            eprintln!("{}", poo);
        }
    }
    //TODO, make sure this works.// if let Ok(queue_res) = queue_client.put_message(new_queue_b64).into_future().await {
    //     eprintln!("{:?}", queue_res);
    // }
    Ok(())
}

pub fn make_headers() -> Result<HeaderMap, Errors> {
    let mut headers = HeaderMap::new();
    let api_key: Cow<str> = match env::var("PJMKEY") {
        Ok(val) => Cow::Owned(val),
        Err(_) => return Err(Errors::MissingEnvVar),
    };
    let api_key: &str = &api_key;
    let header_value = HeaderValue::from_str(api_key)
        .unwrap_or_else(|_| panic!("atheadervalue with {}", &api_key.len()));
    headers.insert("Ocp-Apim-Subscription-Key", header_value);
    Ok(headers)
}

pub fn make_object_store() -> object_store::azure::MicrosoftAzure {
    MicrosoftAzureBuilder::from_env()
        .with_container_name("pjm")
        .build()
        .expect("making object store")
}

pub fn make_req_client() -> Client {
    Client::builder()
        .default_headers(make_headers().expect("making headers in client maker"))
        .gzip(true)
        .build()
        .expect("making client")
}

pub fn make_queue_client(queue_name: &str) -> QueueClient {
    let account = std::env::var("AZURE_QUEUE_ACCOUNT").expect("missing STORAGE_ACCOUNT");
    let access_key = std::env::var("AZURE_QUEUE_KEY").expect("missing STORAGE_ACCESS_KEY");
    let storage_credentials = StorageCredentials::access_key(account.clone(), access_key);
    let queue_service = QueueServiceClient::new(account, storage_credentials);
    queue_service.queue_client(queue_name)
}
