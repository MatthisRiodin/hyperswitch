use api_models::analytics::ucs_api_events::UcsApiEventsRequest;
use common_utils::errors::ReportSwitchExt;
use error_stack::ResultExt;

use super::events::{get_ucs_api_events, UcsApiEventsResult};
use crate::{errors::AnalyticsResult, types::FiltersError, AnalyticsProvider};

pub async fn ucs_api_events_core(
    pool: &AnalyticsProvider,
    req: UcsApiEventsRequest,
    merchant_id: &common_utils::id_type::MerchantId,
) -> AnalyticsResult<Vec<UcsApiEventsResult>> {
    let data = match pool {
        AnalyticsProvider::Sqlx(_) => Err(FiltersError::NotImplemented(
            "UCS API Events not implemented for SQLX",
        ))
        .attach_printable("SQL Analytics is not implemented for UCS API Events"),
        AnalyticsProvider::Clickhouse(ckh_pool)
        | AnalyticsProvider::CombinedSqlx(_, ckh_pool)
        | AnalyticsProvider::CombinedCkh(_, ckh_pool) => {
            get_ucs_api_events(merchant_id, req, ckh_pool).await
        }
    }
    .switch()?;
    Ok(data)
}
