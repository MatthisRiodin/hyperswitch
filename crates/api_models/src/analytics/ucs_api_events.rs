#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UcsApiEventsRequest {
    pub payment_id: Option<common_utils::id_type::PaymentId>,
    pub refund_id: Option<String>,
}
