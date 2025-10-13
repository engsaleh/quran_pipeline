"""
Quran Data Processing Pipeline
===============================
Professional system for collecting, processing, validating, and exporting Quran data.

Author: [Your Name]
Version: 2.0.0
License: MIT
"""

import asyncio
import aiohttp
import json
import sqlite3
import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict, field
from contextlib import asynccontextmanager
from enum import Enum
import sys

# ============================================================================
# Configuration & Constants
# ============================================================================

class Config:
    """Centralized configuration management."""

    # API Configuration
    API_BASE_URL = "https://api.alquran.cloud/v1"
    API_TIMEOUT = 60
    API_CONNECT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 2

    # HTTP Configuration
    CONNECTION_LIMIT = 10
    CONNECTION_LIMIT_PER_HOST = 5
    KEEPALIVE_TIMEOUT = 30

    # Quran Constants
    TOTAL_SURAHS = 114
    TOTAL_VERSES = 6236

    # File Configuration
    DEFAULT_OUTPUT_DIR = "quran_output"
    LOG_FILE = "quran_pipeline.log"
    DATABASE_FILE = "quran_database.sqlite"

    # Version
    VERSION = "2.0.0"


class RevelationType(Enum):
    """Revelation type enumeration."""
    MECCAN = "meccan"
    MEDINAN = "medinan"


# ============================================================================
# Logging Configuration
# ============================================================================

def setup_logging(log_file: str = Config.LOG_FILE) -> logging.Logger:
    """
    Configure logging system with file and console handlers.

    Args:
        log_file: Path to log file

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("QuranPipeline")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# ============================================================================
# Data Models
# ============================================================================

@dataclass(frozen=True)
class VerseData:
    """Immutable verse data model."""
    surah_number: int
    verse_number: int
    text_simple: str
    text_uthmani: str = ""

    def __post_init__(self):
        """Validate verse data after initialization."""
        if self.surah_number < 1 or self.surah_number > Config.TOTAL_SURAHS:
            raise ValueError(f"Invalid surah number: {self.surah_number}")
        if self.verse_number < 1:
            raise ValueError(f"Invalid verse number: {self.verse_number}")


@dataclass(frozen=True)
class SurahInfo:
    """Immutable Surah information model."""
    number: int
    name_arabic: str
    name_english: str
    revelation_type: RevelationType
    verses_count: int

    def __post_init__(self):
        """Validate Surah data after initialization."""
        if self.number < 1 or self.number > Config.TOTAL_SURAHS:
            raise ValueError(f"Invalid surah number: {self.number}")
        if self.verses_count < 1:
            raise ValueError(f"Invalid verses count: {self.verses_count}")


@dataclass
class ValidationResult:
    """Validation result container."""
    is_valid: bool
    issues: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_issue(self, issue: str) -> None:
        """Add validation issue."""
        self.issues.append(issue)
        self.is_valid = False


# ============================================================================
# Custom Exceptions
# ============================================================================

class QuranPipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class DataCollectionError(QuranPipelineError):
    """Exception raised during data collection."""
    pass


class DataValidationError(QuranPipelineError):
    """Exception raised during data validation."""
    pass


class DataExportError(QuranPipelineError):
    """Exception raised during data export."""
    pass


# ============================================================================
# Data Collection Service
# ============================================================================

class QuranAPIClient:
    """
    Asynchronous HTTP client for Quran API.

    Handles connection management, retries, and error handling.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize API client.

        Args:
            logger: Logger instance (creates new if None)
        """
        self.logger = logger or setup_logging()
        self.session: Optional[aiohttp.ClientSession] = None
        self.base_url = Config.API_BASE_URL

    async def __aenter__(self) -> "QuranAPIClient":
        """Create HTTP session on context entry."""
        timeout = aiohttp.ClientTimeout(
            total=Config.API_TIMEOUT,
            connect=Config.API_CONNECT_TIMEOUT
        )

        connector = aiohttp.TCPConnector(
            limit=Config.CONNECTION_LIMIT,
            limit_per_host=Config.CONNECTION_LIMIT_PER_HOST,
            enable_cleanup_closed=True,
            keepalive_timeout=Config.KEEPALIVE_TIMEOUT
        )

        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": f"QuranPipeline/{Config.VERSION}"}
        )

        self.logger.info("HTTP session initialized")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close session on context exit."""
        if self.session:
            await self.session.close()
            self.logger.info("HTTP session closed")

    async def _make_request(
        self,
        endpoint: str,
        max_retries: int = Config.MAX_RETRIES
    ) -> Dict[str, Any]:
        """
        Make HTTP request with exponential backoff retry.

        Args:
            endpoint: API endpoint path
            max_retries: Maximum number of retry attempts

        Returns:
            Response data as dictionary

        Raises:
            DataCollectionError: If request fails after all retries
        """
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(max_retries):
            try:
                self.logger.debug(
                    f"Request attempt {attempt + 1}/{max_retries}: {url}"
                )

                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data

                    self.logger.warning(
                        f"HTTP {response.status} from {url}"
                    )

            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout on attempt {attempt + 1}")
            except aiohttp.ClientError as e:
                self.logger.warning(f"Client error on attempt {attempt + 1}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")

            # Retry logic
            if attempt < max_retries - 1:
                sleep_time = Config.RETRY_BACKOFF_BASE ** attempt
                self.logger.info(f"Retrying in {sleep_time}s...")
                await asyncio.sleep(sleep_time)
            else:
                raise DataCollectionError(
                    f"Failed to fetch data from {url} after {max_retries} attempts"
                )

    async def get_surahs_info(self) -> List[SurahInfo]:
        """
        Fetch Surah information from API.

        Returns:
            List of SurahInfo objects

        Raises:
            DataCollectionError: If data collection fails
        """
        self.logger.info("Fetching Surah information...")

        data = await self._make_request("surah")

        if data.get("code") != 200:
            raise DataCollectionError(
                f"API error: {data.get('status', 'Unknown error')}"
            )

        surahs = []
        for surah_data in data.get("data", []):
            try:
                surah = SurahInfo(
                    number=surah_data["number"],
                    name_arabic=surah_data["name"],
                    name_english=surah_data["englishName"],
                    revelation_type=RevelationType(
                        surah_data["revelationType"].lower()
                    ),
                    verses_count=surah_data["numberOfAyahs"]
                )
                surahs.append(surah)
            except (KeyError, ValueError) as e:
                self.logger.error(f"Error parsing surah data: {e}")
                continue

        self.logger.info(f"Collected {len(surahs)} surahs")
        return surahs

    async def get_verses(self, edition: str) -> List[VerseData]:
        """
        Fetch verses from API for specific edition.

        Args:
            edition: Edition identifier (e.g., 'quran-simple', 'quran-uthmani')

        Returns:
            List of VerseData objects

        Raises:
            DataCollectionError: If data collection fails
        """
        self.logger.info(f"Fetching verses for edition: {edition}")

        data = await self._make_request(f"quran/{edition}")

        if data.get("code") != 200:
            raise DataCollectionError(
                f"API error: {data.get('status', 'Unknown error')}"
            )

        verses = []
        quran_data = data.get("data", {})

        # Handle nested structure
        surahs_data = quran_data.get("surahs", quran_data)

        for surah in surahs_data:
            surah_number = surah["number"]

            for ayah in surah.get("ayahs", []):
                try:
                    verse = VerseData(
                        surah_number=surah_number,
                        verse_number=ayah["numberInSurah"],
                        text_simple=ayah.get("text", ""),
                        text_uthmani=""
                    )
                    verses.append(verse)
                except (KeyError, ValueError) as e:
                    self.logger.error(
                        f"Error parsing verse {surah_number}:{ayah.get('numberInSurah')}: {e}"
                    )
                    continue

        self.logger.info(f"Collected {len(verses)} verses")
        return verses


# ============================================================================
# Text Processing Service
# ============================================================================

class ArabicTextProcessor:
    """
    Specialized processor for Arabic Quranic text.

    Handles normalization, cleaning, and diacritic removal.
    """

    # Arabic diacritics (Tashkeel)
    DIACRITICS = frozenset({
        '\u064b', '\u064c', '\u064d', '\u064e', '\u064f', '\u0650',  # Vowels
        '\u0651', '\u0652', '\u0653', '\u0654', '\u0655', '\u0656',  # Shadda, Sukun
        '\u0657', '\u0658', '\u0659', '\u065a', '\u065b', '\u065c',  # Additional
        '\u065d', '\u065e', '\u065f', '\u0670', '\u06d6', '\u06d7',  # Stop marks
        '\u06d8', '\u06d9', '\u06da', '\u06db', '\u06dc', '\u06df',
        '\u06e0', '\u06e1', '\u06e2', '\u06e3', '\u06e4', '\u06e7',
        '\u06e8', '\u06ea', '\u06eb', '\u06ec', '\u06ed'
    })

    # Character normalization mapping
    NORMALIZATION_MAP = {
        'أ': 'ا', 'إ': 'ا', 'آ': 'ا',
        'ة': 'ه', 'ى': 'ي',
        'ئ': 'ي', 'ؤ': 'و'
    }

    # Arabic character range pattern
    ARABIC_PATTERN = re.compile(
        r'[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF'
        r'\uFB50-\uFDFF\uFE70-\uFEFF\s\u06F0-\u06F9]'
    )

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize processor."""
        self.logger = logger or setup_logging()

    def normalize_unicode(self, text: str) -> str:
        """
        Apply Unicode normalization (NFKC).

        Args:
            text: Input text

        Returns:
            Normalized text
        """
        if not text:
            return ""

        normalized = unicodedata.normalize('NFKC', text)
        normalized = re.sub(r'\s+', ' ', normalized.strip())
        return normalized

    def remove_diacritics(self, text: str) -> str:
        """
        Remove all Arabic diacritics from text.

        Args:
            text: Input text with diacritics

        Returns:
            Text without diacritics
        """
        if not text:
            return ""

        return ''.join(char for char in text if char not in self.DIACRITICS)

    def clean_arabic_text(self, text: str) -> str:
        """
        Comprehensive Arabic text cleaning.

        Args:
            text: Raw Arabic text

        Returns:
            Cleaned and normalized text
        """
        if not text:
            return ""

        # Remove BOM
        text = text.lstrip('\ufeff')

        # Normalize Unicode
        text = self.normalize_unicode(text)

        # Keep only Arabic characters and spaces
        text = ''.join(self.ARABIC_PATTERN.findall(text))

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text.strip())

        return text

    def merge_verse_texts(
        self,
        simple_verses: List[VerseData],
        uthmani_verses: List[VerseData]
    ) -> List[VerseData]:
        """
        Merge and process verses from different sources.

        Args:
            simple_verses: Verses with simple text
            uthmani_verses: Verses with Uthmani text

        Returns:
            List of merged VerseData objects
        """
        self.logger.info("Merging verse texts...")

        # Create lookup dictionary for simple verses
        simple_lookup = {
            (v.surah_number, v.verse_number): v
            for v in simple_verses
        }

        merged_verses = []
        missing_count = 0

        for uthmani_verse in uthmani_verses:
            key = (uthmani_verse.surah_number, uthmani_verse.verse_number)

            simple_verse = simple_lookup.get(key)

            if not simple_verse:
                self.logger.warning(
                    f"Missing simple text for verse {key[0]}:{key[1]}"
                )
                missing_count += 1
                continue

            # Process texts
            cleaned_uthmani = self.clean_arabic_text(uthmani_verse.text_simple)
            cleaned_simple_source = self.clean_arabic_text(simple_verse.text_simple)
            final_simple = self.remove_diacritics(cleaned_simple_source)

            # Create merged verse
            merged_verse = VerseData(
                surah_number=simple_verse.surah_number,
                verse_number=simple_verse.verse_number,
                text_simple=final_simple,
                text_uthmani=cleaned_uthmani
            )
            merged_verses.append(merged_verse)

        if missing_count > 0:
            self.logger.warning(f"Total missing verses: {missing_count}")

        self.logger.info(f"Successfully merged {len(merged_verses)} verses")
        return merged_verses


# ============================================================================
# Validation Service
# ============================================================================

class QuranDataValidator:
    """
    Comprehensive data validation service.

    Validates completeness, correctness, and text quality.
    """

    # Reference verse counts per Surah (official Quran statistics)
    VERSES_PER_SURAH = {
        1: 7, 2: 286, 3: 200, 4: 176, 5: 120, 6: 165, 7: 206, 8: 75,
        9: 129, 10: 109, 11: 123, 12: 111, 13: 43, 14: 52, 15: 99, 16: 128,
        17: 111, 18: 110, 19: 98, 20: 135, 21: 112, 22: 78, 23: 118, 24: 64,
        25: 77, 26: 227, 27: 93, 28: 88, 29: 69, 30: 60, 31: 34, 32: 30,
        33: 73, 34: 54, 35: 45, 36: 83, 37: 182, 38: 88, 39: 75, 40: 85,
        41: 54, 42: 53, 43: 89, 44: 59, 45: 37, 46: 35, 47: 38, 48: 29,
        49: 18, 50: 45, 51: 60, 52: 49, 53: 62, 54: 55, 55: 78, 56: 96,
        57: 29, 58: 22, 59: 24, 60: 13, 61: 14, 62: 11, 63: 11, 64: 18,
        65: 12, 66: 12, 67: 30, 68: 52, 69: 52, 70: 44, 71: 28, 72: 28,
        73: 20, 74: 56, 75: 40, 76: 31, 77: 50, 78: 40, 79: 46, 80: 42,
        81: 29, 82: 19, 83: 36, 84: 25, 85: 22, 86: 17, 87: 19, 88: 26,
        89: 30, 90: 20, 91: 15, 92: 21, 93: 11, 94: 8, 95: 8, 96: 19,
        97: 5, 98: 8, 99: 8, 100: 11, 101: 11, 102: 8, 103: 3, 104: 9,
        105: 5, 106: 4, 107: 7, 108: 3, 109: 6, 110: 3, 111: 5, 112: 4,
        113: 5, 114: 6
    }

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize validator."""
        self.logger = logger or setup_logging()

    def validate_completeness(
        self,
        surahs: List[SurahInfo],
        verses: List[VerseData]
    ) -> ValidationResult:
        """
        Validate data completeness against reference statistics.

        Args:
            surahs: List of Surah information
            verses: List of verses

        Returns:
            ValidationResult with completeness check
        """
        self.logger.info("Validating data completeness...")

        result = ValidationResult(is_valid=True)

        # Validate Surah count
        if len(surahs) != Config.TOTAL_SURAHS:
            result.add_issue(
                f"Incorrect surah count: expected {Config.TOTAL_SURAHS}, "
                f"got {len(surahs)}"
            )

        # Validate total verse count
        if len(verses) != Config.TOTAL_VERSES:
            result.add_issue(
                f"Incorrect verse count: expected {Config.TOTAL_VERSES}, "
                f"got {len(verses)}"
            )

        # Group verses by Surah
        verses_by_surah = {}
        for verse in verses:
            verses_by_surah.setdefault(verse.surah_number, []).append(
                verse.verse_number
            )

        # Validate each Surah
        for surah in surahs:
            expected_count = self.VERSES_PER_SURAH[surah.number]
            actual_verses = verses_by_surah.get(surah.number, [])
            actual_count = len(actual_verses)

            if actual_count != expected_count:
                result.add_issue(
                    f"Surah {surah.number} ({surah.name_arabic}): "
                    f"expected {expected_count} verses, got {actual_count}"
                )

            # Validate verse sequence
            if actual_verses:
                sorted_verses = sorted(actual_verses)
                expected_sequence = list(range(1, expected_count + 1))

                if sorted_verses != expected_sequence:
                    missing = set(expected_sequence) - set(sorted_verses)
                    if missing:
                        result.add_issue(
                            f"Surah {surah.number}: missing verses {sorted(missing)}"
                        )

        # Store metadata
        result.metadata = {
            'total_surahs': len(surahs),
            'total_verses': len(verses),
            'issues_count': len(result.issues)
        }

        if result.is_valid:
            self.logger.info("✓ Data completeness validated successfully")
        else:
            self.logger.error(f"✗ Found {len(result.issues)} completeness issues")

        return result

    def validate_text_quality(self, verses: List[VerseData]) -> ValidationResult:
        """
        Validate text quality (presence, encoding, length).

        Args:
            verses: List of verses to validate

        Returns:
            ValidationResult with quality check
        """
        self.logger.info("Validating text quality...")

        result = ValidationResult(is_valid=True)
        issues_detail = []

        for verse in verses:
            verse_issues = []

            # Check text presence
            if not verse.text_simple or not verse.text_simple.strip():
                verse_issues.append("empty_simple_text")

            if not verse.text_uthmani or not verse.text_uthmani.strip():
                verse_issues.append("empty_uthmani_text")

            # Check Arabic characters
            if verse.text_simple:
                if not re.search(r'[\u0600-\u06FF]', verse.text_simple):
                    verse_issues.append("no_arabic_characters")

            # Check text length
            if verse.text_simple:
                text_len = len(verse.text_simple.strip())
                if text_len < 3:
                    verse_issues.append("text_too_short")
                elif text_len > 1000:
                    verse_issues.append("text_too_long")

            if verse_issues:
                issues_detail.append({
                    'surah': verse.surah_number,
                    'verse': verse.verse_number,
                    'problems': verse_issues
                })
                result.add_issue(
                    f"Verse {verse.surah_number}:{verse.verse_number} - "
                    f"{', '.join(verse_issues)}"
                )

        result.metadata = {
            'total_checked': len(verses),
            'problematic_verses': len(issues_detail),
            'issues_detail': issues_detail[:10]  # First 10 for brevity
        }

        if result.is_valid:
            self.logger.info("✓ Text quality validated successfully")
        else:
            self.logger.warning(
                f"Found quality issues in {len(issues_detail)} verses"
            )

        return result


# ============================================================================
# Export Service
# ============================================================================

class QuranDataExporter:
    """
    Data export service supporting multiple formats.

    Exports to JSON and SQLite database.
    """

    def __init__(
        self,
        output_dir: str = Config.DEFAULT_OUTPUT_DIR,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize exporter.

        Args:
            output_dir: Output directory path
            logger: Logger instance
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logger or setup_logging()
        self.db_path = self.output_dir / Config.DATABASE_FILE

    def _create_database_schema(self, conn: sqlite3.Connection) -> None:
        """
        Create database schema with proper indexes.

        Args:
            conn: Database connection
        """
        cursor = conn.cursor()

        # Surahs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS surahs (
                number INTEGER PRIMARY KEY,
                name_arabic TEXT NOT NULL,
                name_english TEXT NOT NULL,
                revelation_type TEXT NOT NULL,
                verses_count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Verses table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS verses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                surah_number INTEGER NOT NULL,
                verse_number INTEGER NOT NULL,
                text_simple TEXT NOT NULL,
                text_uthmani TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (surah_number) REFERENCES surahs (number),
                UNIQUE(surah_number, verse_number)
            )
        """)

        # Metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_verses_surah "
            "ON verses(surah_number)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_verses_number "
            "ON verses(verse_number)"
        )

        conn.commit()

    def export_to_database(
        self,
        surahs: List[SurahInfo],
        verses: List[VerseData]
    ) -> None:
        """
        Export data to SQLite database.

        Args:
            surahs: List of Surah information
            verses: List of verses

        Raises:
            DataExportError: If export fails
        """
        self.logger.info(f"Exporting to database: {self.db_path}")

        try:
            with sqlite3.connect(self.db_path) as conn:
                self._create_database_schema(conn)
                cursor = conn.cursor()

                # Insert Surahs
                surah_data = [
                    (
                        s.number,
                        s.name_arabic,
                        s.name_english,
                        s.revelation_type.value,
                        s.verses_count
                    )
                    for s in surahs
                ]

                cursor.executemany("""
                    INSERT OR REPLACE INTO surahs
                    (number, name_arabic, name_english, revelation_type, verses_count)
                    VALUES (?, ?, ?, ?, ?)
                """, surah_data)

                # Insert verses
                verse_data = [
                    (v.surah_number, v.verse_number, v.text_simple, v.text_uthmani)
                    for v in verses
                ]

                cursor.executemany("""
                    INSERT OR REPLACE INTO verses
                    (surah_number, verse_number, text_simple, text_uthmani)
                    VALUES (?, ?, ?, ?)
                """, verse_data)

                # Insert metadata
                metadata = [
                    ('last_updated', datetime.now().isoformat()),
                    ('total_surahs', str(len(surahs))),
                    ('total_verses', str(len(verses))),
                    ('version', Config.VERSION)
                ]

                cursor.executemany("""
                    INSERT OR REPLACE INTO metadata (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, metadata)

                conn.commit()

            self.logger.info(
                f"✓ Exported {len(surahs)} surahs and {len(verses)} verses to database"
            )

        except sqlite3.Error as e:
            raise DataExportError(f"Database export failed: {e}")

    def export_to_json(
        self,
        surahs: List[SurahInfo],
        verses: List[VerseData]
    ) -> Tuple[Path, Path]:
        """
        Export data to JSON files (complete and simple versions).

        Args:
            surahs: List of Surah information
            verses: List of verses

        Returns:
            Tuple of (complete_json_path, simple_json_path)

        Raises:
            DataExportError: If export fails
        """
        self.logger.info("Exporting to JSON files...")

        try:
            # Group verses by Surah
            verses_by_surah = {}
            for verse in verses:
                verses_by_surah.setdefault(verse.surah_number, []).append(verse)

            # Sort verses within each Surah
            for surah_verses in verses_by_surah.values():
                surah_verses.sort(key=lambda v: v.verse_number)

            # Build complete data structure
            quran_data = {
                'metadata': {
                    'title': 'Holy Quran - Complete Data',
                    'version': Config.VERSION,
                    'generated_at': datetime.now().isoformat(),
                    'total_surahs': len(surahs),
                    'total_verses': len(verses),
                    'sources': [
                        f'{Config.API_BASE_URL}/quran/quran-simple',
                        f'{Config.API_BASE_URL}/quran/quran-uthmani'
                    ]
                },
                'surahs': []
            }

            # Build Surah data
            for surah in sorted(surahs, key=lambda s: s.number):
                surah_verses = verses_by_surah.get(surah.number, [])

                surah_data = {
                    'number': surah.number,
                    'name': {
                        'arabic': surah.name_arabic,
                        'english': surah.name_english
                    },
                    'revelation_type': surah.revelation_type.value,
                    'verses_count': surah.verses_count,
                    'verses': [
                        {
                            'number': verse.verse_number,
                            'text': {
                                'simple': verse.text_simple,
                                'uthmani': verse.text_uthmani
                            }
                        }
                        for verse in surah_verses
                    ]
                }

                quran_data['surahs'].append(surah_data)

            # Export complete version
            complete_file = self.output_dir / 'quran_complete.json'
            with open(complete_file, 'w', encoding='utf-8') as f:
                json.dump(quran_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"✓ Complete data exported to {complete_file}")

            # Build simple version (text_simple only)
            simple_data = {
                'metadata': quran_data['metadata'].copy(),
                'surahs': [
                    {
                        'number': surah['number'],
                        'name': surah['name'],
                        'revelation_type': surah['revelation_type'],
                        'verses': [
                            {
                                'number': verse['number'],
                                'text': verse['text']['simple']
                            }
                            for verse in surah['verses']
                        ]
                    }
                    for surah in quran_data['surahs']
                ]
            }
            simple_data['metadata']['title'] = 'Holy Quran - Simple Text'

            # Export simple version
            simple_file = self.output_dir / 'quran_simple.json'
            with open(simple_file, 'w', encoding='utf-8') as f:
                json.dump(simple_data, f, ensure_ascii=False, indent=2)

            self.logger.info(f"✓ Simple data exported to {simple_file}")

            return complete_file, simple_file

        except (IOError, OSError) as e:
            raise DataExportError(f"JSON export failed: {e}")

    def export_statistics(
        self,
        surahs: List[SurahInfo],
        verses: List[VerseData]
    ) -> Path:
        """
        Generate and export detailed statistics.

        Args:
            surahs: List of Surah information
            verses: List of verses

        Returns:
            Path to statistics file

        Raises:
            DataExportError: If export fails
        """
        self.logger.info("Generating statistics...")

        try:
            # Calculate statistics
            total_words = sum(len(v.text_simple.split()) for v in verses)
            total_chars = sum(len(v.text_simple.replace(' ', '')) for v in verses)

            meccan_count = sum(
                1 for s in surahs if s.revelation_type == RevelationType.MECCAN
            )
            medinan_count = len(surahs) - meccan_count

            # Group verses by Surah for detailed stats
            verses_by_surah = {}
            for verse in verses:
                verses_by_surah.setdefault(verse.surah_number, []).append(verse)

            statistics = {
                'summary': {
                    'generated_at': datetime.now().isoformat(),
                    'version': Config.VERSION,
                    'total_surahs': len(surahs),
                    'total_verses': len(verses),
                    'total_words': total_words,
                    'total_characters': total_chars,
                    'meccan_surahs': meccan_count,
                    'medinan_surahs': medinan_count,
                    'average_verses_per_surah': round(len(verses) / len(surahs), 2),
                    'average_words_per_verse': round(total_words / len(verses), 2)
                },
                'surahs_detailed': [
                    {
                        'number': surah.number,
                        'name_arabic': surah.name_arabic,
                        'name_english': surah.name_english,
                        'revelation_type': surah.revelation_type.value,
                        'verses_count': surah.verses_count,
                        'word_count': sum(
                            len(v.text_simple.split())
                            for v in verses_by_surah.get(surah.number, [])
                        ),
                        'character_count': sum(
                            len(v.text_simple.replace(' ', ''))
                            for v in verses_by_surah.get(surah.number, [])
                        )
                    }
                    for surah in sorted(surahs, key=lambda s: s.number)
                ]
            }

            # Export statistics
            stats_file = self.output_dir / 'quran_statistics.json'
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(statistics, f, ensure_ascii=False, indent=2)

            self.logger.info(f"✓ Statistics exported to {stats_file}")

            return stats_file

        except (IOError, OSError) as e:
            raise DataExportError(f"Statistics export failed: {e}")


# ============================================================================
# Main Pipeline Orchestrator
# ============================================================================

class QuranPipeline:
    """
    Main pipeline orchestrator.

    Coordinates data collection, processing, validation, and export.
    """

    def __init__(self, output_dir: str = Config.DEFAULT_OUTPUT_DIR):
        """
        Initialize pipeline.

        Args:
            output_dir: Output directory for exported files
        """
        self.output_dir = output_dir
        self.logger = setup_logging()

        # Initialize services
        self.text_processor = ArabicTextProcessor(self.logger)
        self.validator = QuranDataValidator(self.logger)
        self.exporter = QuranDataExporter(output_dir, self.logger)

    def _print_header(self) -> None:
        """Print pipeline header."""
        print("\n" + "=" * 80)
        print("بسم الله الرحمن الرحيم")
        print("Quran Data Processing Pipeline v" + Config.VERSION)
        print("=" * 80 + "\n")

    def _print_section(self, step: int, title: str) -> None:
        """Print section header."""
        print(f"\n{'─' * 80}")
        print(f"Step {step}: {title}")
        print(f"{'─' * 80}")

    def _print_summary(
        self,
        duration: float,
        surahs_count: int,
        verses_count: int
    ) -> None:
        """Print execution summary."""
        print("\n" + "=" * 80)
        print("✓ Pipeline completed successfully!")
        print("=" * 80)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Output directory: {self.output_dir}/")
        print(f"Total surahs: {surahs_count}")
        print(f"Total verses: {verses_count}")
        print("\nGenerated files:")
        print("  • quran_complete.json - Complete Quran data")
        print("  • quran_simple.json - Simple text version")
        print("  • quran_database.sqlite - SQLite database")
        print("  • quran_statistics.json - Detailed statistics")
        print(f"  • {Config.LOG_FILE} - Execution log")
        print("\nالحمد لله رب العالمين")
        print("=" * 80 + "\n")

    async def execute(self) -> None:
        """
        Execute complete pipeline.

        Raises:
            QuranPipelineError: If pipeline execution fails
        """
        start_time = datetime.now()
        self._print_header()

        try:
            # Step 1: Data Collection
            self._print_section(1, "Data Collection")

            async with QuranAPIClient(self.logger) as api_client:
                # Collect data concurrently
                surahs_task = api_client.get_surahs_info()
                simple_task = api_client.get_verses('quran-simple')
                uthmani_task = api_client.get_verses('quran-uthmani')

                surahs, simple_verses, uthmani_verses = await asyncio.gather(
                    surahs_task, simple_task, uthmani_task
                )

            print(f"✓ Collected {len(surahs)} surahs")
            print(f"✓ Collected {len(simple_verses)} simple verses")
            print(f"✓ Collected {len(uthmani_verses)} uthmani verses")

            if not (surahs and simple_verses and uthmani_verses):
                raise DataCollectionError("Failed to collect complete data")

            # Step 2: Text Processing
            self._print_section(2, "Text Processing & Merging")

            merged_verses = self.text_processor.merge_verse_texts(
                simple_verses, uthmani_verses
            )
            print(f"✓ Merged and processed {len(merged_verses)} verses")

            # Step 3: Data Validation
            self._print_section(3, "Data Validation")

            completeness_result = self.validator.validate_completeness(
                surahs, merged_verses
            )

            if not completeness_result.is_valid:
                print(f" Found {len(completeness_result.issues)} completeness issues:")
                for issue in completeness_result.issues[:5]:
                    print(f"  • {issue}")
                if len(completeness_result.issues) > 5:
                    remaining = len(completeness_result.issues) - 5
                    print(f"  ... and {remaining} more issues")

                raise DataValidationError("Data completeness validation failed")

            print("✓ Data completeness validated")

            quality_result = self.validator.validate_text_quality(merged_verses)

            if quality_result.is_valid:
                print("✓ Text quality validated")
            else:
                print(f" Found quality issues in {quality_result.metadata['problematic_verses']} verses")
                # Continue despite quality issues (non-critical)

            # Step 4: Data Export
            self._print_section(4, "Data Export")

            # Export to database
            self.exporter.export_to_database(surahs, merged_verses)
            print("✓ Exported to SQLite database")

            # Export to JSON
            complete_json, simple_json = self.exporter.export_to_json(
                surahs, merged_verses
            )
            print(f"✓ Exported to JSON files")

            # Export statistics
            stats_file = self.exporter.export_statistics(surahs, merged_verses)
            print(f"✓ Generated statistics")

            # Print summary
            duration = (datetime.now() - start_time).total_seconds()
            self._print_summary(duration, len(surahs), len(merged_verses))

        except QuranPipelineError as e:
            self.logger.error(f"Pipeline failed: {e}")
            print(f"\n✗ Pipeline failed: {e}")
            print(f"Check {Config.LOG_FILE} for details")
            raise

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            print(f"\n✗ Unexpected error: {e}")
            print(f"Check {Config.LOG_FILE} for details")
            raise QuranPipelineError(f"Unexpected error: {e}")


# ============================================================================
# Entry Point
# ============================================================================

async def main() -> None:
    """Main entry point."""
    try:
        pipeline = QuranPipeline()
        await pipeline.execute()

    except KeyboardInterrupt:
        print("\n\n Pipeline interrupted by user")
        sys.exit(1)

    except QuranPipelineError:
        sys.exit(1)

    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
