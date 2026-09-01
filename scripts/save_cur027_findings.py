import asyncio
import sys

from _common import get_db_url

sys.path.insert(0, "/home/shtorm/memory-server/src")
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from memory_server.api.remember import remember
from memory_server.providers.sqlite_provider import SQLiteProvider

DB_URL = get_db_url()
SOURCE = "curiosity-worker"
SESSION = "cron_20260824_cur027_booking_confirmation_corpus"

def rebuild(provider):
    engine = create_async_engine(DB_URL, echo=False, connect_args={"timeout": 60})
    provider._engine = engine
    provider._session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

FACTS = [
    (
        "Booking confirmation corpus — normalized model",
        "should_include",
        "Use a canonical model with document and booking identifiers, traveler names, supplier/status, "
        "price/currency/taxes, air segments, hotel stay, transfer, and document metadata. Concur models itinerary as "
        "bookings containing segments; Travelport separates itinerary/leg/flight segment. Treat these as "
        "normalization references rather than a universal supplier schema.",
        0.95,
    ),
    (
        "Booking confirmation corpus — baggage",
        "must_model",
        "Store baggage entitlement per passenger and per air segment, preserving piece-vs-weight notation and the "
        "source text. IATA documents that baggage varies by airline, cabin, route, and may use piece or weight "
        "concepts; an absent baggage field is unknown, not zero.",
        0.96,
    ),
    (
        "Booking confirmation corpus — OCR pipeline",
        "requires",
        "Keep the original PDF/image, rendered pages, OCR text, and normalized extraction as separate artifacts. "
        "Born-digital PDFs should use direct text/layout extraction; scans can use OCRmyPDF/Tesseract, but its "
        "documented limitations include reading order, language sensitivity, degraded scans, and lack of semantic "
        "structure.",
        0.96,
    ),
    (
        "Booking confirmation corpus — evidence",
        "requires",
        "Every extracted field should carry source SHA-256, page, raw span, normalized coordinates/polygon, "
        "engine/version, confidence, and transform. Google Document AI and Azure Document Intelligence expose "
        "confidence and page polygons; this evidence shape supports auditable discrepancy review.",
        0.95,
    ),
    (
        "Booking confirmation corpus — acceptance tests",
        "should_measure",
        "Evaluate field-level exact/normalized match, precision/recall/F1, date/time normalization, localization, "
        "omissions, hallucinations, and discrepancy detection with blocking-error false negatives. Use deterministic "
        "cross-field rules plus human review for low confidence or conflicts; global confidence thresholds alone are "
        "unsafe. ParseBench supports this omission/hallucination/reading-order/semantic-formatting/visual-grounding "
        "framing.",
        0.95,
    ),
]



async def main():
    provider = SQLiteProvider(url=DB_URL)
    rebuild(provider)
    ids=[]
    try:
        for subject, predicate, obj, confidence in FACTS:
            metadata={
                "evidence":
                {
                    "method":
                    "web research across official/vendor documentation and document-AI benchmarks",
                    "sources":
                    [
                        "CUR-027 research 2026-08-24",
                    ],
                    "session_id":
                    SESSION,
                    "claim_type":
                    "fact",
                },
                "source_date":
                "2026-08-24",
                "tags":
                [
                    "curiosity-worker",
                    "cur-027",
                    "booking-confirmation",
                    "ocr",
                    "document-ai",
                    "travel-agent",
                    "acceptance-tests",
                ],
            }

            last=None
            for attempt in range(5):
                try:
                    res=await remember(
                        provider=provider,
                        subject=subject,
                        predicate=predicate,
                        object=obj,
                        confidence=confidence,
                        source=SOURCE,
                        metadata=metadata,
                    )

                    fact=res["fact"]
                    ids.append(getattr(fact, "id", fact.get("id") if isinstance(fact, dict) else str(fact)))
                    break
                except Exception as exc:
                    last=exc
                    await asyncio.sleep(8*(attempt+1))
            else:
                if last is None:
                    raise RuntimeError("remember() failed without an exception")
                raise last
        print("saved", ids)
    finally:
        await provider.close()

asyncio.run(main())
