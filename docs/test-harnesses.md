# Paired refactor characterization harnesses

The C07, C08, and C09 application tests are generated/compared against legacy behavior using external subject/oracle inputs. TODO: procedure not recovered from the refactor project. Do not invent oracle-generation steps. These modules intentionally skip during ordinary pytest collection unless their required environment is provided:

- C07 (`tests/test_application_retrieval.py`): `C07_SUBJECT`, `C07_ORACLE`, `C07_SCRATCH`.
- C08 (`tests/test_application_beliefs.py`): `C08_SUBJECT`, `C08_ORACLE`, `C08_SCRATCH`.
- C09 (`tests/test_application_operations.py`): `C09_SUBJECT`, `C09_ORACLE`, `C09_SCRATCH`.

Run them only from the recovered harness procedure with disposable scratch storage and the appropriate subject/oracle. The tests are not silently treated as passing when that setup is absent.
