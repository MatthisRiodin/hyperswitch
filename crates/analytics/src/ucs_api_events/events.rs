use api_models::analytics::{ucs_api_events::UcsApiEventsRequest, Granularity};
use common_utils::errors::ReportSwitchExt;
use error_stack::ResultExt;
use time::PrimitiveDateTime;

use crate::{
    query::{Aggregate, GroupByClause, QueryBuilder, ToSql, Window},
    types::{AnalyticsCollection, AnalyticsDataSource, FiltersError, FiltersResult, LoadRow},
};
pub trait UcsApiEventLogAnalytics: LoadRow<UcsApiEventsResult> {}

pub async fn get_ucs_api_events<T>(
    merchant_id: &common_utils::id_type::MerchantId,
    query_param: UcsApiEventsRequest,
    pool: &T,
) -> FiltersResult<Vec<UcsApiEventsResult>>
where
    T: AnalyticsDataSource + UcsApiEventLogAnalytics,
    PrimitiveDateTime: ToSql<T>,
    AnalyticsCollection: ToSql<T>,
    Granularity: GroupByClause<T>,
    Aggregate<&'static str>: ToSql<T>,
    Window<&'static str>: ToSql<T>,
{
    let mut query_builder: QueryBuilder<T> = QueryBuilder::new(AnalyticsCollection::UcsApiEvents);
    query_builder.add_select_column("*").switch()?;

    query_builder
        .add_filter_clause("merchant_id", merchant_id)
        .switch()?;

    if let Some(payment_id) = query_param.payment_id {
        query_builder
            .add_filter_clause("payment_id", &payment_id)
            .switch()?;
    }

    if let Some(refund_id) = query_param.refund_id {
        query_builder
            .add_filter_clause("refund_id", &refund_id)
            .switch()?;
    }

    query_builder
        .execute_query::<UcsApiEventsResult, _>(pool)
        .await
        .change_context(FiltersError::QueryBuildingError)?
        .change_context(FiltersError::QueryExecutionFailure)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct UcsApiEventsResult {
    pub merchant_id: common_utils::id_type::MerchantId,
    pub payment_id: Option<String>,
    pub connector_name: Option<String>,
    pub request_id: Option<String>,
    pub flow: String,
    pub request: String,
    #[serde(rename = "masked_response")]
    pub response: Option<String>,
    pub error: Option<String>,
    pub status_code: u16,
    pub latency: Option<u128>,
    pub method: Option<String>,
    pub url: Option<String>,
    pub stage: Option<String>,
    pub refund_id: Option<String>,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub execution_mode: Option<String>,
    #[serde(with = "common_utils::custom_serde::iso8601")]
    pub created_at: PrimitiveDateTime,
}
