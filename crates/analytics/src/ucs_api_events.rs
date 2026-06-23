mod core;
pub mod events;
pub trait UcsApiEventAnalytics: events::UcsApiEventLogAnalytics {}

pub use self::core::ucs_api_events_core;
