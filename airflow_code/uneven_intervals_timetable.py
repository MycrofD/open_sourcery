"""Timetable"""

import calendar
from typing import Optional, Dict, Any
from pendulum import Date, DateTime, Time, timezone

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")


def _days_before_eom(date: DateTime, days):
    return (
        calendar.monthrange(
            date.year,
            date.month,
        )[1]
    ) - days


def _add_month(date: DateTime):
    month = date.month + 1
    year = date.year
    if month > 12:
        year = date.year + 1
        month = month - 12
    new_date = date.set(year=year, month=month)
    return new_date


def _set_1_to_27(start_date: DateTime):
    next_date = _add_month(start_date)
    next_start = next_date.set(day=1, hour=0)
    next_end = next_date.set(
        year=next_date.year,
        month=next_date.month,
        day=_days_before_eom(next_date, 3),
        hour=0,
    ).replace(tzinfo=UTC)
    return {"next_start": next_start, "next_end": next_end}


def _set_1_to_31(start_date: DateTime):
    # Set to hour 1 to avoid collisions in the UI that happen when
    # two schedules have the same data interval start date
    next_start = start_date.set(day=1, hour=1).replace(tzinfo=UTC)
    next_date = next_start
    next_end = next_start.set(
        year=next_date.year,
        month=next_date.month,
        day=_days_before_eom(next_date, 0),
        hour=1,
    ).replace(tzinfo=UTC)
    return {"next_start": next_start, "next_end": next_end}


def _set_start_next_1st_to_27(start_date: DateTime):
    if start_date.day != 1:
        next_date = _add_month(start_date)
        start_date = next_date.set(day=1, hour=0).replace(tzinfo=UTC)
    start_date = start_date.set(hour=0).replace(tzinfo=UTC)
    next_end = start_date.set(day=_days_before_eom(start_date, 3))
    return {"next_start": start_date, "next_end": next_end}


class UnevenIntervalsTimetable(Timetable):
    """Timetable Class defining the schedules"""

    def __init__(self, schedule_at: int, day_1_to_x: int = None) -> None:
        self._schedule_at = schedule_at
        self._day_1_to_x = day_1_to_x

    def serialize(self) -> Dict[str, Any]:
        """Need to tell airflow how to serialize it to stored in DB"""
        # return {"schedule_at": self._schedule_at.isoformat()}
        return {"schedule_at": self._schedule_at}

    @classmethod
    def deserialize(cls, value: Dict[str, Any]) -> Timetable:
        """Need to tell airflow how to deserialize instance"""
        return cls(schedule_at=value["schedule_at"])

    @property
    def summary(self) -> str:
        return f"Run 3 days before end of month and the first of following month, at {self._schedule_at}"

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        # run_after is a pendulum.DateTime object that indicates when the DAG is
        # externally triggered
        start = run_after.set(day=1)
        return DataInterval(start=start, end=run_after)

    def next_dagrun_info(
        self,
        *,
        # is a DataInterval instance indicating the this DAG’s previous non-manually-triggered run
        last_automated_data_interval: Optional[DataInterval],
        # contains earliest like dag's start_date and latest like dag's end_date
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if (
            last_automated_data_interval is not None
        ):  # There was a previous run on the regular schedule.
            if last_automated_data_interval.end.day == _days_before_eom(
                last_automated_data_interval.end, 0
            ):  # if last run was triggered for the full month
                next_dates = _set_1_to_27(last_automated_data_interval.start)

            else:
                next_dates = _set_1_to_31(last_automated_data_interval.start)
            next_start = next_dates["next_start"]
            next_end = next_dates["next_end"]

        else:
            if restriction.earliest is None:  # No start_date. Don't schedule.
                return None

            if (
                restriction.latest is not None
                and restriction.earliest > restriction.latest
            ):
                return None  # Over the DAG's scheduled end; don't schedule.

            next_dates = _set_start_next_1st_to_27(restriction.earliest)
            next_start = next_dates["next_start"]
            next_end = next_dates["next_end"]

            if not restriction.catchup:
                # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(
                    restriction.earliest,
                    DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC),
                )

        return DagRunInfo(
            data_interval=DataInterval(start=next_start, end=next_end),
            run_after=next_end.add(days=1).set(hour=0).add(hours=self._schedule_at),
        )


class UnevenIntervalsTimetablePlugin(AirflowPlugin):
    """Class defining the Airflow Plugin that instantiates the Timetables"""

    name = "uneven_intervals_timetable_plugin"
    timetables = [UnevenIntervalsTimetable]
