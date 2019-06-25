use actix_service::{Service, Transform};
use actix_web::*;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use crate::metrics::WEB_REQUEST_HISTOGRAM;
use futures::future::{ok, FutureResult};
use futures::prelude::*;

#[derive(Clone, Default)]
pub struct MetricsMiddleware;

impl<S, B> Transform<S> for MetricsMiddleware
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = DefaultMetricsMiddleware<S>;
    type Future = FutureResult<Self::Transform, Self::InitError>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(DefaultMetricsMiddleware {
            service,
        })
    }
}

pub struct DefaultMetricsMiddleware<S> {
    service: S,
}

impl<S, B> Service for DefaultMetricsMiddleware<S>
where
    S: Service<Request = ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
{
    type Request = ServiceRequest;
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.service.poll_ready()
    }

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        let timer = WEB_REQUEST_HISTOGRAM.with_label_values(&["all"]).start_timer();
        Box::new(self.service.call(req).map(move |res| {
            timer.observe_duration();
            res
        }))
    }
}
