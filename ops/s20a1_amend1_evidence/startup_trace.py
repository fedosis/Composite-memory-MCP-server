"""Read-only diagnostic: expose the assertion that Daemon wraps as LeaseError."""
import sys

observed = set()


def trace(frame, event, arg):
    if event == 'exception':
        kind, value, _ = arg
        if kind is AssertionError and 'S19P_STARTUP_PREPROVIDER' in str(value):
            observed.add((frame.f_code.co_filename, frame.f_lineno, str(value)))
    return trace


def pytest_sessionstart(session):
    sys.settrace(trace)


def pytest_sessionfinish(session, exitstatus):
    sys.settrace(None)
    for filename, line, message in sorted(observed):
        print(f'OBSERVED_ASSERTION {filename}:{line}: {message}')
