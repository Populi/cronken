from typing import Optional, Union

from pydantic import BaseModel, model_validator


class CronArgs(BaseModel):
    cronstring: Optional[str] = None
    year: Optional[str] = None
    month: Optional[str] = None
    day: Optional[str] = None
    week: Optional[str] = None
    day_of_week: Optional[str] = None
    hour: Optional[str] = None
    minute: Optional[str] = None
    second: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    timezone: Optional[str] = None
    jitter: Optional[int] = None

    @model_validator(mode="after")
    def cronstring_or_kwargs(self):
        time_fields = ("year", "month", "day", "week", "day_of_week", "hour", "minute", "second")
        assert self.cronstring or any(
            getattr(self, x) for x in time_fields
        ), f"Either cronstring or one of the time-based fields ({', '.join(time_fields)}) must be set"
        return self


class JobArgs(BaseModel):
    cmd: str
    lock: Union[str, bool]
    ttl: int

    @model_validator(mode="after")
    def cmd_not_empty(self):
        assert self.cmd, "Job command must not be empty"
        return self


class JobState(BaseModel):
    paused: bool = False
    validation_errors: Optional[str] = None


class JobDef(BaseModel):
    job_args: JobArgs
    cron_args: CronArgs
    job_state: Optional[JobState] = None