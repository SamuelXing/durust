//! Saga with compensation: book flight → hotel → car. If a later booking fails,
//! the bookings already made are rolled back with compensating steps.
//!
//! Every booking *and* every cancellation is a durable `#[durare::step]`, so a
//! crash mid-saga resumes exactly where it left off and compensations also run
//! at most once. This uses the in-memory backend for a self-contained demo; a
//! real deployment would use Postgres or SQLite so the saga survives a restart.
//!
//! ```text
//! cargo run --example saga
//! ```

use durare::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone)]
struct Trip {
    traveler: String,
    /// When true the car booking fails, triggering compensation.
    car_unavailable: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct Booking {
    flight: String,
    hotel: String,
    car: String,
}

#[durare::step]
async fn book_flight(ctx: &DurableContext, traveler: String) -> Result<String> {
    println!("  >> booking flight for {traveler}");
    Ok(format!("FL-{traveler}"))
}

#[durare::step]
async fn book_hotel(ctx: &DurableContext, traveler: String) -> Result<String> {
    println!("  >> booking hotel for {traveler}");
    Ok(format!("HT-{traveler}"))
}

#[durare::step]
async fn book_car(ctx: &DurableContext, traveler: String, available: bool) -> Result<String> {
    if !available {
        println!("  !! car unavailable for {traveler} — booking fails");
        return Err(Error::app("no cars available"));
    }
    println!("  >> booking car for {traveler}");
    Ok(format!("CR-{traveler}"))
}

#[durare::step]
async fn cancel_hotel(ctx: &DurableContext, hotel_id: String) -> Result<()> {
    println!("  << compensating: cancel hotel {hotel_id}");
    Ok(())
}

#[durare::step]
async fn cancel_flight(ctx: &DurableContext, flight_id: String) -> Result<()> {
    println!("  << compensating: cancel flight {flight_id}");
    Ok(())
}

#[durare::workflow]
async fn book_trip(ctx: DurableContext, trip: Trip) -> Result<Booking> {
    let flight = book_flight(&ctx, trip.traveler.clone()).await?;
    let hotel = book_hotel(&ctx, trip.traveler.clone()).await?;

    // The step that may fail. On error, unwind the bookings already made with
    // compensating steps — each durable, so a crash during rollback also
    // resumes cleanly — then propagate the failure.
    let car = match book_car(&ctx, trip.traveler.clone(), !trip.car_unavailable).await {
        Ok(car) => car,
        Err(e) => {
            cancel_hotel(&ctx, hotel).await?;
            cancel_flight(&ctx, flight).await?;
            return Err(e);
        }
    };

    Ok(Booking { flight, hotel, car })
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // Happy path: all three bookings succeed.
    println!("== trip 1: everything available ==");
    let booked: Booking = engine
        .start_with(
            BookTrip,
            Trip {
                traveler: "ada".to_string(),
                car_unavailable: false,
            },
            WorkflowOptions::with_id("trip-ada"),
        )
        .await?
        .await?;
    println!("[booked] {booked:?}\n");

    // Failure path: no car, so the flight and hotel are rolled back.
    println!("== trip 2: no car — saga compensates ==");
    let handle = engine
        .start_with(
            BookTrip,
            Trip {
                traveler: "grace".to_string(),
                car_unavailable: true,
            },
            WorkflowOptions::with_id("trip-grace"),
        )
        .await?;
    match handle.await {
        Ok(b) => println!("[booked] {b:?}"),
        Err(e) => println!("[rolled back] booking did not complete: {e}"),
    }

    Ok(())
}
