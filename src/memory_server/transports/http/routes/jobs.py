"""Direct jobs.get descriptor; no HTTP mount or profile resolver."""

from __future__ import annotations

import re

from memory_server.gateway_contracts import dto as d
from memory_server.gateway_contracts.ports import JobPort, RequestContext
from memory_server.transports.http.errors import HttpError
from memory_server.transports.http.guards import ReadRouteSpec
from memory_server.transports.http.routes.inspectors import _fields

JobsGetCommand = d.COMMAND_TYPES["jobs.get"]


def _id(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{48}", value) is None:
        raise HttpError("E_VALIDATION")
    return value


def _result(value):
    try:
        if type(value) is not d.Job:
            raise ValueError()
        return d.Job.model_validate(_fields(value), strict=True)
    except (ValueError, TypeError, AttributeError):
        raise HttpError("E_INTERNAL") from None


def _parse(request, body, principal):
    if body != b"" or request.scope.get("query_string", b"") != b"":
        raise HttpError("E_VALIDATION")
    return JobsGetCommand(
        operation="jobs.get", profile_id=None, path_ids={"id": _id(request.path_params.get("id"))}, payload=d.Empty()
    )


class JobReadAdapter:
    def __init__(self, jobs: JobPort):
        self.jobs = jobs

    async def execute(self, ctx: RequestContext, command: JobsGetCommand) -> d.Job:
        try:
            if type(command) is not JobsGetCommand:
                raise ValueError()
            command = JobsGetCommand.model_validate(_fields(command), strict=True)
            if command.profile_id is not None or set(command.path_ids) != {"id"}:
                raise ValueError()
            ident = _id(command.path_ids["id"])
        except (ValueError, TypeError, AttributeError):
            raise HttpError("E_VALIDATION") from None
        job = _result(await self.jobs.get(ctx, ident))
        if job.id != ident or job.profile_id != ctx.profile_id:
            raise HttpError("E_INTERNAL")
        return job


def create_job_routes():
    return (ReadRouteSpec("jobs.get", "GET", "/api/v1/jobs/{id}", "index.manage", _parse, _result),)
