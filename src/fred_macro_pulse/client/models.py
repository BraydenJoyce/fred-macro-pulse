from datetime import date

from pydantic import BaseModel


class Observation(BaseModel):
    date: date
    value: str  # raw string from API; "." means missing


class SeriesMetadata(BaseModel):
    id: str
    title: str
    units: str
    frequency: str
    seasonal_adjustment: str
    notes: str | None = None


class ObservationsResponse(BaseModel):
    series_id: str
    observations: list[Observation]
