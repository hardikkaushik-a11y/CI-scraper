"""
enrich.py — Competitive Hiring Intelligence Enricher v5
───────────────────────────────────────────────────────
• Reads jobs_raw.csv, merges with existing jobs_enriched.csv (preserves First_Seen)
• Only classifies NEW rows — saves API cost
• Re-classifies carried rows that still have "Other" product_focus or function
• Claude Haiku for Function/Product_Focus classification (batches of 20)
• Claude Sonnet for strategic signal inference (signals.json)
• Falls back to comprehensive regex if no ANTHROPIC_API_KEY
• 365-day rolling window — drops jobs older than 1 year
• "Other" / "Unknown" never appear in output — always mapped to specific category
• Outputs: jobs_enriched.csv + signals.json
"""

import csv
import json
import os
import re
import time
from datetime import date, datetime, timedelta
from collections import defaultdict

import httpx

# ══════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-5-20250115"
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = "deepseek-chat"  # V4-Flash
MAX_JOB_AGE_DAYS = 365

# Allowed values — anything outside these sets gets remapped via fallback
ALLOWED_FUNCTIONS = {
    "Engineering", "Data/Analytics", "AI/ML & Vector", "Sales", "Marketing",
    "Product Management", "Operations", "Design", "Finance",
    "Security", "Customer Success", "Solution Engineering", "Partners/Alliances",
}
# Functions to exclude entirely — these jobs are not relevant to CI analysis
EXCLUDED_FUNCTIONS = {"Legal", "People/HR"}
# Job title patterns to exclude — these are not real job postings
JUNK_TITLE_PATTERNS = {
    r"download",
    r"product\s+(page|download)",
    r"careers\s+(page|hub)",
    r"job\s+board",
    r"apply\s+now",
    r"contact\s+us",
}
ALLOWED_PRODUCT_FOCUS = {
    "Data Quality", "Data Observability", "Data Governance", "ETL/Integration",
    "Streaming / Real-time", "ML/AI infra", "Platform / Infra",
    "Vector / Embedding", "Database / Storage", "Cloud Infrastructure",
    "Security / Compliance", "Analytics / BI", "Developer Tools",
    "Go-to-Market", "Corporate Functions", "Product Management",
    "Vector / AI",  # Actian's upcoming product line
}

# ══════════════════════════════════════════════════════════════════════════
# JUNK DETECTION
# ══════════════════════════════════════════════════════════════════════════

def is_junk_job(title: str) -> bool:
    """Check if a job title is junk (product pages, downloads, etc.)"""
    title_lower = (title or "").lower()
    for pattern in JUNK_TITLE_PATTERNS:
        if re.search(pattern, title_lower):
            return True
    return False

FIELDNAMES = [
    "Company", "Job Title", "Job Link", "Location",
    "Posting Date", "Days Since Posted",
    "Function", "Seniority",
    "Company_Group", "Product_Focus", "Product_Focus_Tokens",
    "Primary_Skill", "Extracted_Skills",
    "Relevancy_to_Actian", "Trend_Score",
    "First_Seen", "Last_Seen", "Description",
]

# ══════════════════════════════════════════════════════════════════════════
# COMPANY GROUPS (loaded from competitors.csv when available)
# ══════════════════════════════════════════════════════════════════════════

COMPANY_GROUP_KEYWORDS = {
    "Data Intelligence":    ["collibra", "informatica", "atlan", "alation", "datagalaxy",
                             "data.world", "ataccama", "qlik", "castordoc"],
    "Data Observability":   ["anomalo", "bigeye", "monte carlo", "sifflet", "decube"],
    "ETL/Connectors":       ["fivetran", "matillion", "boomi", "syniti", "precisely"],
    "Warehouse/Processing": ["snowflake", "databricks", "teradata", "vertica", "exasol",
                             "firebolt", "influxdata", "couchbase", "alteryx", "cloudera",
                             "mongodb", "pentaho"],
    "Monitoring/Platforms": ["datadog"],
    "Vector DB / AI":       ["pinecone", "weaviate", "qdrant", "zilliz"],
    "Enterprise":           ["salesforce", "ibm", "sap", "oracle", "amazon"],
}

_CSV_GROUP_MAP: dict[str, str] = {}


def _load_csv_groups(path: str = "competitors.csv"):
    global _CSV_GROUP_MAP
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            company = row.get("Company", "").strip()
            group = row.get("Company_Group", "").strip()
            if company and group:
                _CSV_GROUP_MAP[company.lower()] = group


def classify_company_group(company: str) -> str:
    low = company.lower()
    if low in _CSV_GROUP_MAP:
        return _CSV_GROUP_MAP[low]
    for group, keywords in COMPANY_GROUP_KEYWORDS.items():
        if any(k in low for k in keywords):
            return group
    return "Other"

# ══════════════════════════════════════════════════════════════════════════
# SENIORITY DETECTION — COMPREHENSIVE
# ══════════════════════════════════════════════════════════════════════════

def detect_seniority(title: str) -> str:
    """Detect seniority from job title. Much broader matching than v3."""
    if not title:
        return "Mid"  # Default to Mid instead of Unknown
    t = title.lower()

    # Director+ (C-suite, VP, Director, Head)
    if re.search(r'\b(chief|cto|ceo|cfo|coo|cio|ciso|evp|svp)\b', t):
        return "Director+"
    if re.search(r'\b(vp|vice.?president|director|head of|general manager|gm)\b', t):
        return "Director+"
    if re.search(r'\b(fellow|distinguished)\b', t):
        return "Director+"

    # Principal/Staff
    if re.search(r'\b(principal|staff)\b', t):
        return "Principal/Staff"

    # Senior
    if re.search(r'\b(senior|sr\.?|lead\b|team lead)', t):
        return "Senior"

    # Manager
    if re.search(r'\b(manager|mgr|supervisor|group lead)\b', t):
        return "Manager"

    # Entry
    if re.search(r'\b(junior|jr\.?|entry|graduate|new grad|early career|associate)\b', t):
        return "Entry"

    # Intern — exclude "internal" (negative lookahead for "al")
    if re.search(r'\b(intern(?!al)|internship|werkstudent|trainee|co-?op|apprentice)\b', t):
        return "Intern"

    # Mid-level explicit markers
    if re.search(r'\b(mid.?level| ii\b| iii\b| iv\b|level [234])\b', t):
        return "Mid"

    # If it's a recognizable role with no seniority marker, it's Mid
    if re.search(r'\b(engineer|developer|analyst|scientist|designer|specialist|consultant|coordinator|administrator|accountant|recruiter|writer|strategist)\b', t):
        return "Mid"

    return "Mid"  # Default to Mid — "Unknown" is not useful

# ══════════════════════════════════════════════════════════════════════════
# SKILL EXTRACTION
# ══════════════════════════════════════════════════════════════════════════

SKILL_MAP = {
    # ═══ LANGUAGES ═══
    "python": "PYTHON", "sql": "SQL", "java": "JAVA", "scala": "SCALA",
    "go": "GO", "golang": "GO", "rust": "RUST", "kotlin": "KOTLIN",
    "javascript": "JAVASCRIPT", "js": "JAVASCRIPT", "typescript": "TYPESCRIPT", "ts": "TYPESCRIPT",
    "c++": "C++", "cpp": "C++", "c#": "C#", "csharp": "C#",
    "ruby": "RUBY", "php": "PHP", "swift": "SWIFT", "objective-c": "OBJECTIVE_C",
    ".net": ".NET", "dotnet": ".NET", "clojure": "CLOJURE", "elixir": "ELIXIR",
    "haskell": "HASKELL", "perl": "PERL", "r": "R",

    # ═══ CLOUD & INFRA ═══
    "aws": "AWS", "amazon web services": "AWS", "gcp": "GCP", "google cloud": "GCP",
    "azure": "AZURE", "microsoft azure": "AZURE",
    "kubernetes": "KUBERNETES", "k8s": "KUBERNETES", "docker": "DOCKER",
    "terraform": "TERRAFORM", "ansible": "ANSIBLE", "helm": "HELM",
    "jenkins": "JENKINS", "gitlab": "GITLAB", "github actions": "GITHUB_ACTIONS",
    "cicd": "CI/CD", "ci/cd": "CI/CD", "devops": "DEVOPS", "sre": "SRE",

    # ═══ DATA WAREHOUSES & DATABASES ═══
    "snowflake": "SNOWFLAKE", "databricks": "DATABRICKS", "redshift": "REDSHIFT",
    "bigquery": "BIGQUERY", "postgres": "POSTGRES", "postgresql": "POSTGRES",
    "mysql": "MYSQL", "mongodb": "MONGODB", "cassandra": "CASSANDRA",
    "redis": "REDIS", "dynamodb": "DYNAMODB", "couchdb": "COUCHDB",
    "elasticsearch": "ELASTICSEARCH", "opensearch": "OPENSEARCH",
    "clickhouse": "CLICKHOUSE", "vertica": "VERTICA", "greenplum": "GREENPLUM",
    "oracle": "ORACLE", "sql server": "SQL_SERVER", "db2": "DB2",
    "cockroachdb": "COCKROACHDB", "tidb": "TIDB", "crdb": "COCKROACHDB",

    # ═══ STREAMING & MESSAGING ═══
    "kafka": "KAFKA", "flink": "FLINK", "spark": "SPARK", "airflow": "AIRFLOW",
    "dagster": "DAGSTER", "dbt": "DBT", "stream processing": "STREAM_PROCESSING",
    "rabbitmq": "RABBITMQ", "redis streams": "REDIS_STREAMS", "pubsub": "PUBSUB",
    "kinesis": "KINESIS", "eventhub": "EVENTHUB", "message queue": "MESSAGE_QUEUE",
    "nifi": "NIFI", "kafka connect": "KAFKA_CONNECT",

    # ═══ ETL/ELT & INTEGRATION ═══
    "etl": "ETL", "elt": "ELT", "talend": "TALEND", "informatica": "INFORMATICA",
    "fivetran": "FIVETRAN", "stitch": "STITCH", "dbt": "DBT",
    "integration": "INTEGRATION", "data pipeline": "DATA_PIPELINE",
    "change data capture": "CDC", "cdc": "CDC", "replication": "REPLICATION",

    # ═══ DATA GOVERNANCE & QUALITY ═══
    "governance": "GOVERNANCE", "lineage": "LINEAGE", "metadata": "METADATA",
    "data quality": "DATA_QUALITY", "data catalog": "DATA_CATALOG",
    "data discovery": "DATA_DISCOVERY", "master data": "MASTER_DATA",
    "observability": "OBSERVABILITY", "monitoring": "MONITORING", "dataops": "DATAOPS",
    "data observability": "DATA_OBSERVABILITY", "data stewardship": "DATA_STEWARDSHIP",

    # ═══ AI/ML & VECTOR ═══
    "ml": "ML", "machine learning": "ML", "ai": "AI", "artificial intelligence": "AI",
    "llm": "LLM", "large language model": "LLM", "gpt": "GPT",
    "rag": "RAG", "retrieval augmented": "RAG", "nlp": "NLP", "natural language": "NLP",
    "computer vision": "COMPUTER_VISION", "cv": "COMPUTER_VISION",
    "vector": "VECTOR", "embedding": "EMBEDDING", "embeddings": "EMBEDDING",
    "mlops": "MLOPS", "model": "MODEL", "deep learning": "DEEP_LEARNING",
    "pytorch": "PYTORCH", "tensorflow": "TENSORFLOW", "keras": "KERAS",
    "scikit-learn": "SCIKIT_LEARN", "huggingface": "HUGGINGFACE", "openai": "OPENAI",
    "langchain": "LANGCHAIN", "prompt engineering": "PROMPT_ENGINEERING",
    "vector database": "VECTOR_DB", "weaviate": "WEAVIATE", "pinecone": "PINECONE",
    "milvus": "MILVUS", "qdrant": "QDRANT",

    # ═══ BI & ANALYTICS ═══
    "tableau": "TABLEAU", "looker": "LOOKER", "power bi": "POWER_BI", "powerbi": "POWER_BI",
    "qlik": "QLIK", "microstrategy": "MICROSTRATEGY", "informatica": "INFORMATICA",
    "alteryx": "ALTERYX", "sisense": "SISENSE", "perforce": "PERFORCE",
    "analytics": "ANALYTICS", "business intelligence": "BUSINESS_INTELLIGENCE", "bi": "BUSINESS_INTELLIGENCE",

    # ═══ MONITORING & OBSERVABILITY ═══
    "prometheus": "PROMETHEUS", "grafana": "GRAFANA", "datadog": "DATADOG",
    "newrelic": "NEWRELIC", "elastic": "ELASTIC", "splunk": "SPLUNK",
    "dynatrace": "DYNATRACE", "sumologic": "SUMOLOGIC", "logz.io": "LOGZ_IO",
    "sentry": "SENTRY", "honeycomb": "HONEYCOMB", "lightstep": "LIGHTSTEP",
    "tracing": "TRACING", "logging": "LOGGING", "metrics": "METRICS",

    # ═══ WEB & API ═══
    "api": "API", "rest": "REST", "restful": "REST", "graphql": "GRAPHQL",
    "grpc": "GRPC", "http": "HTTP", "https": "HTTPS", "websocket": "WEBSOCKET",
    "microservices": "MICROSERVICES", "service mesh": "SERVICE_MESH", "istio": "ISTIO",
    "envoy": "ENVOY", "api gateway": "API_GATEWAY", "kong": "KONG",

    # ═══ SECURITY & COMPLIANCE ═══
    "security": "SECURITY", "compliance": "COMPLIANCE", "gdpr": "GDPR",
    "hipaa": "HIPAA", "soc2": "SOC2", "pci": "PCI", "encryption": "ENCRYPTION",
    "authentication": "AUTHENTICATION", "authorization": "AUTHORIZATION", "iam": "IAM",
    "ssl": "SSL", "tls": "TLS", "zero trust": "ZERO_TRUST", "threat detection": "THREAT_DETECTION",

    # ═══ DISTRIBUTED SYSTEMS & ARCHITECTURE ═══
    "distributed systems": "DISTRIBUTED_SYSTEMS", "concurrency": "CONCURRENCY",
    "scalability": "SCALABILITY", "fault tolerance": "FAULT_TOLERANCE",
    "consensus": "CONSENSUS", "raft": "RAFT", "paxos": "PAXOS",
    "load balancing": "LOAD_BALANCING", "cache": "CACHE", "caching": "CACHE",

    # ═══ MODERN FRAMEWORKS ═══
    "react": "REACT", "vue": "VUE", "angular": "ANGULAR", "next.js": "NEXT_JS",
    "django": "DJANGO", "flask": "FLASK", "fastapi": "FASTAPI", "spring": "SPRING",
    "node.js": "NODE_JS", "nodejs": "NODE_JS", "express": "EXPRESS",
    "rails": "RAILS", "asp.net": "ASP_NET", "laravel": "LARAVEL",

    # ═══ DATA FORMATS & PROTOCOLS ═══
    "json": "JSON", "xml": "XML", "protobuf": "PROTOBUF", "avro": "AVRO",
    "parquet": "PARQUET", "csv": "CSV", "yaml": "YAML", "toml": "TOML",
    "arrow": "ARROW", "orc": "ORC", "iceberg": "ICEBERG", "delta": "DELTA",

    # ═══ TESTING & QA ═══
    "testing": "TESTING", "unit testing": "UNIT_TESTING", "integration testing": "INTEGRATION_TESTING",
    "jest": "JEST", "pytest": "PYTEST", "junit": "JUNIT", "selenium": "SELENIUM",
    "qa": "QA", "quality assurance": "QA", "automation": "AUTOMATION",

    # ═══ PRODUCT & DESIGN ═══
    "product": "PRODUCT", "product management": "PRODUCT_MANAGEMENT", "analytics": "ANALYTICS",
    "user experience": "UX", "ux": "UX", "ui": "UI", "design": "DESIGN",

    # ═══ MANAGEMENT & OPERATIONS ═══
    "leadership": "LEADERSHIP", "management": "MANAGEMENT", "scrum": "SCRUM",
    "agile": "AGILE", "kanban": "KANBAN", "project management": "PROJECT_MANAGEMENT",
}

# ═══ SKILL VARIATIONS & ALIASES ═══
# Maps rare/alternative spellings to canonical forms
SKILL_ALIASES = {
    "gke": "KUBERNETES", "aks": "KUBERNETES", "eks": "KUBERNETES",
    "s3": "AWS", "ec2": "AWS", "lambda": "AWS",
    "gce": "GCP", "app engine": "GCP", "cloud run": "GCP",
    "vnet": "AZURE", "cosmos db": "AZURE",
    "ml ops": "MLOPS", "ml-ops": "MLOPS",
    "real time": "REALTIME", "realtime": "REALTIME",
    "vector db": "VECTOR_DB", "vectordb": "VECTOR_DB",
    "gen ai": "AI", "genai": "AI",
}

TOKEN_RE = re.compile(r'\b([A-Za-z0-9_\-\.#\+]+)\b')


def extract_skills(title: str) -> list[str]:
    """
    Extract skills from job title with multi-level matching:
    1. Multi-word phrase matching (highest priority)
    2. Single-word token matching
    3. Alias resolution
    4. Acronym/abbreviation inference
    """
    text = (title or "").lower()
    skills = []
    seen = set()

    # LEVEL 1: Multi-word phrases (highest priority - do first)
    for phrase, canon in SKILL_MAP.items():
        if " " in phrase and phrase in text and canon not in seen:
            skills.append(canon)
            seen.add(canon)

    # LEVEL 2: Single-word token matching
    tokens = TOKEN_RE.findall(text)
    for tok in tokens:
        clean_tok = tok.strip(".-").lower()
        # Try direct match in SKILL_MAP
        if clean_tok in SKILL_MAP:
            canon = SKILL_MAP[clean_tok]
            if canon not in seen:
                skills.append(canon)
                seen.add(canon)
        # Try alias lookup
        elif clean_tok in SKILL_ALIASES:
            canon = SKILL_ALIASES[clean_tok]
            if canon not in seen:
                skills.append(canon)
                seen.add(canon)

    # LEVEL 3: Aggressive pattern-based inference
    text_lower = text.lower()

    # ═══ ENGINEERING ROLES ═══
    if re.search(r'\bengine|architect|developer\b', text_lower):
        if "ENGINEERING" not in seen:
            skills.append("ENGINEERING")
            seen.add("ENGINEERING")

    # Data Engineering
    if re.search(r'data.*engineer|pipeline|etl|warehouse|lake', text_lower):
        if "ETL" not in seen:
            skills.append("ETL")
            seen.add("ETL")
        if "SQL" not in seen:
            skills.append("SQL")
            seen.add("SQL")
        if "PYTHON" not in seen:
            skills.append("PYTHON")
            seen.add("PYTHON")

    # Platform/Infrastructure Engineering
    if re.search(r'platform|infrastructure|devops|sre|cloud|reliability', text_lower):
        if "DEVOPS" not in seen:
            skills.append("DEVOPS")
            seen.add("DEVOPS")
        if "KUBERNETES" not in seen and re.search(r'kubernetes|k8s|container', text_lower):
            skills.append("KUBERNETES")
            seen.add("KUBERNETES")
        if "DOCKER" not in seen and "docker" in text_lower:
            skills.append("DOCKER")
            seen.add("DOCKER")

    # Backend/Systems Engineering
    if re.search(r'backend|systems|software|core', text_lower):
        if "ENGINEERING" not in seen:
            skills.append("ENGINEERING")
            seen.add("ENGINEERING")
        if re.search(r'java|go|rust|c\+\+|python', text_lower):
            pass  # Already added via token matching
        elif "PYTHON" not in seen:
            skills.append("PYTHON")
            seen.add("PYTHON")

    # ═══ DATABASE/STORAGE ROLES ═══
    if re.search(r'database|data.*store|warehouse|storage|sql|postgres|mysql', text_lower):
        if "SQL" not in seen:
            skills.append("SQL")
            seen.add("SQL")
        # Specific databases added via token matching

    # ═══ ANALYTICS/DATA ROLES ═══
    if re.search(r'\banalyst|analytics|bi|business intelligence|report|dashboard', text_lower):
        if "ANALYTICS" not in seen:
            skills.append("ANALYTICS")
            seen.add("ANALYTICS")
        if "SQL" not in seen:
            skills.append("SQL")
            seen.add("SQL")
        if re.search(r'tableau|looker|power.*bi|qlik', text_lower):
            pass  # Already added via token matching
        elif "TABLEAU" not in seen and "tableau" in text_lower:
            skills.append("TABLEAU")
            seen.add("TABLEAU")

    # ═══ ML/AI ROLES ═══
    if re.search(r'\bml\b|machine learning|ai\b|artificial intelligence|data scientist|mlops', text_lower):
        if "ML" not in seen:
            skills.append("ML")
            seen.add("ML")
        if "AI" not in seen:
            skills.append("AI")
            seen.add("AI")
        if "PYTHON" not in seen:
            skills.append("PYTHON")
            seen.add("PYTHON")
        if "SQL" not in seen:
            skills.append("SQL")
            seen.add("SQL")
        if re.search(r'pytorch|tensorflow|keras', text_lower):
            pass  # Already added

    # ═══ DATA GOVERNANCE/OBSERVABILITY ═══
    if re.search(r'governance|lineage|metadata|catalog|observability|quality|monitoring', text_lower):
        if "GOVERNANCE" not in seen and "governance" in text_lower:
            skills.append("GOVERNANCE")
            seen.add("GOVERNANCE")
        if "OBSERVABILITY" not in seen and "observability" in text_lower:
            skills.append("OBSERVABILITY")
            seen.add("OBSERVABILITY")
        if "MONITORING" not in seen and "monitoring" in text_lower:
            skills.append("MONITORING")
            seen.add("MONITORING")
        if "SQL" not in seen:
            skills.append("SQL")
            seen.add("SQL")

    # ═══ SECURITY/COMPLIANCE ═══
    if re.search(r'security|compliance|gdpr|hipaa|soc2|encryption|auth', text_lower):
        if "SECURITY" not in seen:
            skills.append("SECURITY")
            seen.add("SECURITY")
        if "COMPLIANCE" not in seen and "compliance" in text_lower:
            skills.append("COMPLIANCE")
            seen.add("COMPLIANCE")

    # ═══ PRODUCT/MANAGEMENT ═══
    if re.search(r'\bproduct|pm\b|manager|lead', text_lower):
        if "PRODUCT" not in seen and "product" in text_lower:
            skills.append("PRODUCT")
            seen.add("PRODUCT")
        if "MANAGEMENT" not in seen and re.search(r'manager|lead|director', text_lower):
            skills.append("MANAGEMENT")
            seen.add("MANAGEMENT")
        if "ANALYTICS" not in seen and "analytics" in text_lower:
            skills.append("ANALYTICS")
            seen.add("ANALYTICS")

    # ═══ SALES/GTM ═══
    if re.search(r'sales|gtm|go.to.market|account.*manager|customer', text_lower):
        if "ANALYTICS" not in seen:  # Sales reps should know analytics tools
            skills.append("ANALYTICS")
            seen.add("ANALYTICS")

    # ═══ CLOUD/MULTI-CLOUD PATTERNS ═══
    if re.search(r'multi.cloud|cloud', text_lower):
        cloud_found = any(s in skills for s in ("AWS", "GCP", "AZURE"))
        if not cloud_found and "DEVOPS" not in seen:
            skills.append("DEVOPS")
            seen.add("DEVOPS")

    # LEVEL 4: Expand single skills to related skills (skill stacking)
    expanded = list(skills)
    if "PYTHON" in skills and "SQL" not in seen:
        expanded.append("SQL")
        seen.add("SQL")
    if "JAVA" in skills and "MICROSERVICES" not in seen:
        expanded.append("MICROSERVICES")
        seen.add("MICROSERVICES")
    if any(s in skills for s in ("AWS", "GCP", "AZURE")) and "DEVOPS" not in seen:
        expanded.append("DEVOPS")
        seen.add("DEVOPS")
    if "KAFKA" in skills and "STREAMING" not in seen:
        expanded.append("STREAMING")
        seen.add("STREAMING")

    # Return up to 15 skills (increased from 12)
    return expanded[:15]

# ══════════════════════════════════════════════════════════════════════════
# SCORING
# ══════════════════════════════════════════════════════════════════════════

ACTIAN_RELEVANT = {
    "ETL", "DBT", "SQL", "AWS", "AZURE", "GCP", "SNOWFLAKE", "DATABRICKS",
    "POSTGRES", "STREAMING", "KAFKA", "RUST", "GO", "GOVERNANCE",
    "OBSERVABILITY", "LINEAGE", "DATA_QUALITY", "INTEGRATION", "REALTIME",
    "VECTOR", "EMBEDDING", "RAG", "LLM", "MLOPS", "FLINK",
}

HIGH_RELEVANCE_PRODUCTS = {
    "ETL/Integration", "Data Governance", "Data Observability",
    "Streaming / Real-time", "Vector / Embedding", "Vector / AI",
    "Data Quality",
}

SENIORITY_WEIGHT = {
    "Director+": 3.0, "Principal/Staff": 2.5, "Senior": 1.5,
    "Manager": 1.5, "Mid": 0.8, "Entry": 0.3, "Intern": 0.1,
}

ACTIAN_GEOS = {"united states", "us", "usa", "germany", "india", "uk",
               "singapore", "canada", "remote", "new york", "san francisco",
               "seattle", "austin", "boston", "chicago", "denver", "atlanta",
               "london", "berlin", "bangalore", "bengaluru", "toronto", "sydney"}


def compute_relevancy(skills: list[str], location: str, product_focus: str,
                      seniority: str, company_group: str = "") -> float:
    score = 0.0

    # Skill match (most important signal)
    matched = [s for s in skills if s in ACTIAN_RELEVANT]
    score += 3.0 * len(matched)

    # Product focus relevance
    if product_focus in HIGH_RELEVANCE_PRODUCTS:
        score += 5.0
    elif product_focus in ("Platform / Infra", "ML/AI infra"):
        score += 2.0

    # Geography
    loc_low = (location or "").lower()
    if any(g in loc_low for g in ACTIAN_GEOS):
        score += 2.0

    # Seniority (senior hires are more strategic)
    score += SENIORITY_WEIGHT.get(seniority, 0.5)

    # AI/ML boost
    if any(s in skills for s in ("AI", "ML", "MLOPS", "LLM", "RAG", "NLP")):
        score += 2.0

    # Company group proximity
    if company_group in ("ETL/Connectors", "Data Intelligence", "Data Observability"):
        score += 3.0
    elif company_group in ("Warehouse/Processing", "Monitoring/Platforms"):
        score += 1.5

    return round(min(10.0, max(0.0, score / 1.8)), 1)


def compute_trend(title: str, seniority: str) -> float:
    t = (title or "").lower()
    s = 0.0
    if re.search(r'ai|ml|llm|gpt|rag|mlops|genai|gen ai', t):
        s += 2.0
    if re.search(r'stream|kafka|real.?time|flink', t):
        s += 1.5
    if re.search(r'observab|monitor|anomal', t):
        s += 1.5
    if re.search(r'governance|catalog|lineage', t):
        s += 1.0
    if re.search(r'vector|embedding|semantic', t):
        s += 2.0
    if seniority in ("Senior", "Principal/Staff", "Director+"):
        s += 1.0
    return round(min(10.0, s), 2)

# ══════════════════════════════════════════════════════════════════════════
# CLASSIFICATION — COMPREHENSIVE REGEX FALLBACK
# ══════════════════════════════════════════════════════════════════════════

CLASSIFY_SYSTEM = """You are a job classification assistant for a competitive intelligence system at Actian (data integration & analytics). Given job titles and optional description snippets, return ONLY a JSON array.
Each element: {"function": "<one of the function options>", "product_focus": "<one of the product focus options>"}
Function options: Engineering, Data/Analytics, AI/ML & Vector, Sales, Marketing, Product Management, Solution Engineering, Partners/Alliances, Operations, People/HR, Design, Finance, Legal, Security, Customer Success
Product focus options: Data Quality, Data Observability, Data Governance, ETL/Integration, Streaming / Real-time, ML/AI infra, Platform / Infra, Vector / Embedding, Vector / AI, Database / Storage, Cloud Infrastructure, Security / Compliance, Analytics / BI, Developer Tools, Go-to-Market, Corporate Functions, Product Management
CRITICAL RULES — you MUST follow these:
1. NEVER return "Other" or "Unknown" for function or product_focus. These are NOT valid values.
2. Solutions Engineer/Pre-Sales/Sales Engineer/Technical Presales roles → function: Solution Engineering, product_focus: Go-to-Market
3. Partner Manager/Alliance Manager/Channel Manager/Ecosystem roles → function: Partners/Alliances, product_focus: Go-to-Market
4. Sales/BDR/SDR/Account Executive/Account Manager roles → function: Sales, product_focus: Go-to-Market
5. Marketing/Content/Brand/Growth roles → function: Marketing, product_focus: Go-to-Market
6. HR/Finance/Legal/Operations/Admin/Office/Facilities roles → function: People/HR or Finance or Legal or Operations, product_focus: Corporate Functions
7. Product Manager/Program Manager/Design/Scrum/Agile roles → function: Product Management or Design, product_focus: Product Management
8. Vector database, embedding, similarity search roles → product_focus: Vector / AI
9. Generic software engineer/developer with no specific domain → product_focus: Platform / Infra
10. If truly unclear, use Platform / Infra for technical roles or Go-to-Market for business roles.
Return ONLY the JSON array, no other text."""


def _call_claude(model: str, system: str, user_msg: str, max_tokens: int = 4096) -> str:
    if not ANTHROPIC_API_KEY:
        return ""
    try:
        r = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": max_tokens,
                "system": system,
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=90,
        )
        r.raise_for_status()
        return r.json()["content"][0]["text"].strip()
    except Exception as e:
        print(f"  [WARN] Claude API call failed ({model}): {e}")
        return ""


def _call_deepseek(system: str, user_msg: str, max_tokens: int = 1500) -> str:
    if not DEEPSEEK_API_KEY:
        return ""
    try:
        r = httpx.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEEPSEEK_MODEL,
                "max_tokens": max_tokens,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_msg},
                ],
            },
            timeout=90,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  [WARN] DeepSeek API call failed: {e}")
        return ""


def _sanitize_classification(cls: dict, title: str, description: str = "") -> dict:
    """Ensure no 'Other', 'Unknown', or invalid values leak through from Claude API."""
    pf = cls.get("product_focus", "")
    fn = cls.get("function", "")

    # Reject ANY value not in allowed sets — not just "Other"/"Unknown"
    if not pf or pf not in ALLOWED_PRODUCT_FOCUS:
        fallback = _fallback_classify(title, description)
        pf = fallback["product_focus"]

    if not fn or fn not in ALLOWED_FUNCTIONS:
        fallback = _fallback_classify(title, description)
        fn = fallback["function"]

    return {"function": fn, "product_focus": pf}


def classify_batch(titles: list[str], descriptions: list[str] | None = None) -> list[dict]:
    descs = descriptions or [""] * len(titles)
    return [_fallback_classify(t, d) for t, d in zip(titles, descs)]


def _fallback_classify(title: str, description: str = "") -> dict:
    """Regex classification using title + description for best accuracy."""
    t = (title or "").lower()
    d = (description or "").lower()
    # Combined text lets description break ties on ambiguous titles.
    # Title terms still win because ordering is checked on `t` first.
    combined = f"{t} {d}"

    # ── FUNCTION ─────────────────────────────────────────────────────────
    # AI/ML first (most specific)
    if re.search(r'machine learning|ml engineer|ml ops|mlops|llm|ai engineer|ai research|'
                 r'data scientist|deep learning|computer vision|nlp|natural language|'
                 r'vector|embedding|generative ai|gen ?ai|prompt engineer', t):
        fn = "AI/ML & Vector"
    # Security
    elif re.search(r'security|infosec|cyber|penetration|threat|vulnerability|soc analyst|'
                   r'identity|access management|iam\b|ciso', t):
        fn = "Security"
    # Product Management — before Engineering to prevent "product manager, data platform"
    # matching "platform" in the Engineering regex
    elif re.search(r'product manager|product owner|product lead|product director|'
                   r'product market|product strat|product anal|technical product|'
                   r'program manager|project manager|scrum|agile|'
                   r'(?:head|vp|director|chief).{0,10}product|head of product', t):
        fn = "Product Management"
    # Solution Engineering — before Engineering to prevent misclassification
    elif re.search(
        r'solutions? ?engineer|sales engineer|pre.?sales|technical presales|'
        r'presales consultant|demo engineer|field sales engineer|'
        r'technical sales engineer|solutions specialist',
        combined
    ):
        fn = "Solution Engineering"
    # Partners/Alliances — before Sales to prevent partner roles landing in Sales
    elif re.search(
        r'partner (?:manager|director|success|develop|program|lead|executive)|'
        r'(?:channel|alliance|ecosystem) (?:manager|director|develop|partner|sales|lead)|'
        r'technology (?:partner|alliance|channel|ecosystem)|'
        r'partnerships? (?:manager|director|lead|develop|executive)|'
        r'(?:head|vp|director|chief).{0,10}(?:partner|alliance|channel|ecosystem)|'
        r'strategic partner|isv partner|reseller|partner develop',
        combined
    ):
        fn = "Partners/Alliances"
    # Engineering (broad — catches most technical roles)
    elif re.search(r'engineer|developer|devops|sre|software|architect|infra|backend|'
                   r'frontend|full.?stack|platform|systems|reliability|embedded|'
                   r'mobile|ios|android|web dev|qa|test|automation|release|'
                   r'devsecops|cloud|network|database|dba', t):
        fn = "Engineering"
    # Data/Analytics
    elif re.search(r'data analyst|data engineer|analytics|business intel|bi developer|'
                   r'data manager|data arch|data model|etl|dbt|reporting|insight|'
                   r'tableau|looker|power bi|visualization', t):
        fn = "Data/Analytics"
    # Design
    elif re.search(r'design|ux|ui|user experience|user interface|graphic|creative|'
                   r'visual|brand design|product design|interaction', t):
        fn = "Design"
    # Sales
    elif re.search(r'sales|account exec|account manager|business develop|bdr|sdr|'
                   r'revenue|quota|enterprise rep|field rep|solution consult|'
                   r'deal|territory', t):
        fn = "Sales"
    # Customer Success
    elif re.search(r'customer success|csm|customer experience|implementation|'
                   r'onboarding|technical account|tam\b|support engineer|'
                   r'solutions architect|professional services|client', t):
        fn = "Customer Success"
    # Marketing
    elif re.search(r'marketing|content|copywriter|brand|growth|demand gen|'
                   r'communications|pr\b|public relation|social media|seo|sem|'
                   r'event|campaign|field market|product market', t):
        fn = "Marketing"
    # People/HR
    elif re.search(r'hr\b|human resource|talent|recruit|people|compensation|'
                   r'benefits|payroll|dei|diversity|workplace|culture|'
                   r'learning|training|enablement', t):
        fn = "People/HR"
    # Finance
    elif re.search(r'finance|accounting|controller|treasury|tax|audit|'
                   r'financial|fp&a|procurement|accounts payable|billing', t):
        fn = "Finance"
    # Legal
    elif re.search(r'legal|counsel|attorney|compliance|regulatory|policy|'
                   r'privacy|ip\b|intellectual property|contract', t):
        fn = "Legal"
    # Operations
    elif re.search(r'operations|ops\b|logistics|supply chain|facilities|'
                   r'office manager|admin|executive assistant|it support|'
                   r'help desk|service desk|procurement', t):
        fn = "Operations"
    # Dept hint fallback for Greenhouse/Ashby jobs — use dept name to classify
    elif d.startswith("dept: "):
        dept = d[6:].strip()
        if re.search(r'solution|pre.?sales|sales eng', dept):
            fn = "Solution Engineering"
        elif re.search(r'partner|alliance|channel|ecosystem', dept):
            fn = "Partners/Alliances"
        elif re.search(r'engineer|develop|infra|platform|product eng', dept):
            fn = "Engineering"
        elif re.search(r'sales|revenue|account|gtm', dept):
            fn = "Sales"
        elif re.search(r'product', dept):
            fn = "Product Management"
        elif re.search(r'market', dept):
            fn = "Marketing"
        elif re.search(r'success|support|service', dept):
            fn = "Customer Success"
        else:
            fn = "Engineering"
    else:
        # Last resort: try to infer from common suffixes
        if re.search(r'manager|director|head|lead|chief|vp', t):
            fn = "Operations"  # Leadership roles without clear function
        elif re.search(r'specialist|coordinator|administrator|associate', t):
            fn = "Operations"
        else:
            fn = "Engineering"  # At a tech company, ambiguous roles are usually eng

    # ── PRODUCT FOCUS ────────────────────────────────────────────────────
    # Order matters: most specific first

    # Database / Storage
    if re.search(r'database|storage|postgres|mysql|mongo|redis|elasticsearch|'
                 r'cassandra|dynamo|cockroach|timeseries|columnar|warehouse|'
                 r'olap|oltp|query engine|sql engine|dba|data lake', t):
        pf = "Database / Storage"
    # Data Observability
    elif re.search(r'observab|monitor|anomal|alert|incident|on.?call|'
                   r'reliability|uptime|slo|sli|pager|opsgenie', t):
        pf = "Data Observability"
    # Data Governance
    elif re.search(r'governance|catalog|lineage|metadata|glossary|steward|'
                   r'classification|retention|policy|master data|mdm', t):
        pf = "Data Governance"
    # ETL/Integration
    elif re.search(r'etl|elt|integrat|replicat|pipeline|connector|sync|'
                   r'ingest|migration|transformation|data movement|cdc|'
                   r'change data|batch processing', t):
        pf = "ETL/Integration"
    # Streaming / Real-time
    elif re.search(r'stream|kafka|real.?time|flink|pub.?sub|event.?driven|'
                   r'messaging|kinesis|rabbitmq|celery', t):
        pf = "Streaming / Real-time"
    # Vector / AI (Actian's upcoming product line — vector DB, embedding, similarity)
    elif re.search(r'vector|embed|semantic|similarity|nearest neighbor|'
                   r'retrieval|rag|search relevance|vector db|vector database|'
                   r'milvus|pinecone|weaviate|qdrant|zilliz|faiss|ann\b|hnsw', t):
        pf = "Vector / AI"
    # ML/AI infra
    elif re.search(r'ml |machine learn|mlops|model|llm|ai |deep learn|'
                   r'training|inference|gpu|feature store|experiment|'
                   r'data scientist|generative|gen ?ai|prompt|nlp|'
                   r'computer vision|neural|pytorch|tensorflow', t):
        pf = "ML/AI infra"
    # Data Quality
    elif re.search(r'quality|accuracy|testing|validat|regression|qa\b|'
                   r'test engineer|test autom|correctness|integrity', t):
        pf = "Data Quality"
    # Security / Compliance
    elif re.search(r'security|compliance|gdpr|hipaa|soc2|fedramp|'
                   r'encryption|auth|identity|access|iam\b|zero trust|'
                   r'penetration|vulnerability|threat|audit', t):
        pf = "Security / Compliance"
    # Cloud Infrastructure
    elif re.search(r'cloud|aws|gcp|azure|kubernetes|k8s|docker|terraform|'
                   r'infra|devops|sre|reliability|container|ci.?cd|'
                   r'deployment|provisioning|networking', t):
        pf = "Cloud Infrastructure"
    # Analytics / BI
    elif re.search(r'analytics|business intel|reporting|dashboard|'
                   r'visualization|tableau|looker|power bi|insight|'
                   r'data analyst|bi\b|metric|kpi', t):
        pf = "Analytics / BI"
    # Developer Tools
    elif re.search(r'developer experience|dev tool|sdk|api|cli|'
                   r'documentation|technical writer|developer rel|'
                   r'devrel|developer advocate|open source|oss', t):
        pf = "Developer Tools"
    # Platform / Infra (broad catch for generic eng roles at data companies)
    elif re.search(r'platform|backend|frontend|full.?stack|web|mobile|'
                   r'systems|distributed|microservice|architect|software', t):
        pf = "Platform / Infra"
    else:
        # For non-technical roles, infer from function
        if fn in ("Sales", "Marketing", "Customer Success", "Solution Engineering", "Partners/Alliances"):
            pf = "Go-to-Market"
        elif fn in ("People/HR", "Finance", "Legal", "Operations"):
            pf = "Corporate Functions"
        elif fn in ("Product Management", "Design"):
            pf = "Product Management"
        else:
            pf = "Platform / Infra"  # Default for tech companies

    return {"function": fn, "product_focus": pf}

# ══════════════════════════════════════════════════════════════════════════
# STRATEGIC SIGNAL INFERENCE
# ══════════════════════════════════════════════════════════════════════════

SIGNAL_SYSTEM = """You are a senior competitive intelligence analyst at Actian Corporation — a data integration and analytics platform serving enterprise customers with strengths in hybrid data integration, real-time analytics, and legacy modernization.

Your job: analyze hiring patterns to extract premium, analyst-grade intelligence that directly informs Actian's product and go-to-market decisions.

Rules:
- Be specific and sharp. Generic phrases like "monitor closely", "invest in innovation", "stay competitive", or "continue to watch" are BANNED.
- Every insight must include a clear "why" — the reasoning chain behind your inference.
- Tailor every insight to the specific competitor. Never produce recommendations that could apply to any company.
- Think in terms of competitive positioning, capability adjacency, platform vs feature dynamics, and market timing.
- Infer intent from COMBINATIONS of signals (role clusters, seniority mix, function ratios, geo patterns), not single data points.
- For recommended actions: specify whether it's a product action (build, partner, acquire, deprioritize, differentiate), GTM action (target segments, messaging, sales enablement), or timing call (immediate, next quarter, monitor).
- Always return valid JSON."""

# Context-aware product direction labels per company segment
_GROUP_DIRECTION = {
    "Data Observability": {
        "ai":       "AI-Driven Pipeline Intelligence & Anomaly Detection",
        "cloud":    "SaaS-First Data Reliability Platform",
        "infra":    "Enterprise-Scale Observability Infrastructure",
        "gtm":      "SMB & Mid-Market Data Observability Expansion",
        "data_eng": "Deep Pipeline Monitoring for Complex ETL Workflows",
        "security": "Compliance-Ready Observability for Regulated Industries",
        "default":  "Next-Gen Data Pipeline Observability Suite",
    },
    "Warehouse/Processing": {
        "ai":       "AI-Native Lakehouse with Embedded ML Governance",
        "cloud":    "Multi-Cloud Managed Warehouse Platform",
        "infra":    "High-Performance Query Engine & Storage Replatform",
        "gtm":      "Enterprise Warehouse Land-and-Expand Motion",
        "data_eng": "Unified Batch & Streaming Data Processing Engine",
        "security": "Secure Data Warehouse for Regulated Enterprise Workloads",
        "default":  "Modern Cloud Lakehouse Platform Expansion",
    },
    "ETL/Connectors": {
        "ai":       "AI-Augmented Data Integration & Smart Mapping",
        "cloud":    "Cloud-Native ELT with Auto-Provisioned Connectors",
        "infra":    "High-Throughput Real-Time Data Pipeline Platform",
        "gtm":      "Mid-Market Data Integration Push via New Pricing",
        "data_eng": "Next-Gen Connector Ecosystem & Transformation Engine",
        "security": "Enterprise-Grade Secure Data Movement & Lineage",
        "default":  "Expanded Data Integration Connector Marketplace",
    },
    "Data Intelligence": {
        "ai":       "AI-Powered Data Catalog with Automated Governance",
        "cloud":    "Cloud Metadata & Data Discovery as a Service",
        "infra":    "Scalable Metadata Platform for Enterprise Data Estates",
        "gtm":      "Self-Service Data Governance for Business Teams",
        "data_eng": "Automated Data Lineage & Pipeline Governance",
        "security": "Data Privacy & Compliance Intelligence Platform",
        "default":  "Intelligent Data Catalog & Governance Expansion",
    },
    "Vector DB / AI": {
        "ai":       "LLM-Native Vector Platform with Retrieval Augmentation",
        "cloud":    "Fully-Managed Serverless Vector Search Infrastructure",
        "infra":    "High-Scale Vector Index for Production AI Workloads",
        "gtm":      "Developer-Led Expansion into AI Application Market",
        "data_eng": "Hybrid Vector + Relational Data Pipeline Tooling",
        "security": "Secure & Compliant AI Infrastructure for Enterprise",
        "default":  "Next-Gen AI-Native Data Retrieval Platform",
    },
    "Monitoring/Platforms": {
        "ai":       "AIOps & Intelligent Anomaly Detection Platform",
        "cloud":    "Unified Cloud Monitoring & Observability SaaS",
        "infra":    "Platform-Scale Infrastructure Reliability Engineering",
        "gtm":      "Platform Monitoring Expansion into New Verticals",
        "data_eng": "Metrics & Log Pipeline Optimization Engine",
        "security": "Security Observability & Threat Detection Integration",
        "default":  "Comprehensive Platform Observability Suite",
    },
    "Analytics / BI": {
        "ai":       "AI-Augmented BI with Natural Language Query",
        "cloud":    "Embedded Analytics Cloud Platform",
        "infra":    "High-Performance Analytics Engine Replatform",
        "gtm":      "Self-Service Analytics Expansion for Business Users",
        "data_eng": "Real-Time Analytics on Live Operational Data",
        "security": "Governed Analytics for Regulated Enterprise Data",
        "default":  "Modern Self-Service Analytics Platform",
    },
    "Enterprise": {
        "ai":       "AI-First Enterprise Data & Automation Platform",
        "cloud":    "Multi-Cloud Enterprise Data Cloud Migration",
        "infra":    "Enterprise Infrastructure Modernization at Scale",
        "gtm":      "Enterprise Suite Expansion into New Industry Verticals",
        "data_eng": "Enterprise Data Integration & Transformation Overhaul",
        "security": "Enterprise-Grade Data Security & Compliance Suite",
        "default":  "Enterprise Data Platform Modernization Initiative",
    },
}


def _infer_roadmap(company: str, company_group: str, n: int, all_titles: list[str],
                   ai_count: int, cloud_count: int, data_eng_count: int, go_to_market: int,
                   infra_count: int, security_count: int, senior_ratio: float,
                   dom_pf: str, dom_fn: str) -> dict:
    """Infer the most likely next product direction from hiring signal patterns."""
    ai_r       = ai_count / max(n, 1)
    cloud_r    = cloud_count / max(n, 1)
    data_eng_r = data_eng_count / max(n, 1)
    gtm_r      = go_to_market / max(n, 1)
    infra_r    = infra_count / max(n, 1)
    sec_r      = security_count / max(n, 1)

    # Score each direction dimension
    scores = {
        "ai":       ai_r * 3.0 + (0.4 if dom_fn == "AI/ML & Vector" else 0),
        "cloud":    cloud_r * 2.5,
        "data_eng": data_eng_r * 2.5,
        "gtm":      gtm_r * 2.0,
        "infra":    infra_r * 2.0 + (0.3 if senior_ratio > 0.5 else 0),
        "security": sec_r * 3.0,
    }
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    primary_key, primary_score = ranked[0]

    # Get direction label from group context
    group_ctx = _GROUP_DIRECTION.get(company_group, _GROUP_DIRECTION["Enterprise"])
    # Only use a specific key if its score is meaningfully above default threshold
    direction = group_ctx[primary_key] if primary_score > 0.1 else group_ctx["default"]

    # Confidence — based on clarity of hiring signal
    if primary_score > 0.8:
        confidence = "High"  # Strong, focused hiring pattern in one area
    elif primary_score > 0.25:
        confidence = "Medium"  # Clear signal but spread across multiple areas
    else:
        confidence = "Low"  # Limited data or mixed hiring signals

    # Timeline
    if n >= 20 and primary_score > 1.0:
        timeline = "Next 3–6 months"
    elif n >= 8:
        timeline = "Next 6–12 months"
    else:
        timeline = "12+ months out"

    # Build a specific rationale from actual numbers
    evidence = []
    if ai_count >= 2:
        evidence.append(f"{ai_count} AI/ML roles ({round(ai_r*100)}% of hiring)")
    if infra_count >= 2:
        evidence.append(f"{infra_count} platform/infrastructure roles")
    if go_to_market >= 3:
        evidence.append(f"{go_to_market} sales and marketing hires pointing to market readiness")
    if data_eng_count >= 2:
        evidence.append(f"{data_eng_count} data engineering roles")
    if cloud_count >= 2:
        evidence.append(f"{cloud_count} cloud-specialist roles")
    if security_count >= 2:
        evidence.append(f"{security_count} security/compliance roles")
    if senior_ratio > 0.5:
        evidence.append(f"{round(senior_ratio*100)}% senior+ hiring suggesting deliberate strategic build")

    if evidence:
        rationale = (
            f"{company} is signalling this direction through {', '.join(evidence[:3])}. "
            f"{'This volume of hiring in a focused area typically precedes a product launch or major feature announcement within one to two quarters.' if n >= 15 else 'The pattern suggests early-stage investment — laying groundwork before a broader push.'}"
        )
    else:
        rationale = (
            f"With {n} active roles concentrated in {dom_pf}, {company} appears to be "
            f"in steady execution mode — iterating on its existing {company_group.lower()} platform "
            f"without a sharp strategic pivot visible from hiring alone."
        )

    # Watch for: specific leading indicators tied to the primary signal and company context
    watch_map = {
        "ai": [
            f"New AI/ML job postings appearing in product or applied research teams at {company} — would confirm they're building, not just exploring",
            f"{company} publishing technical blog posts about embedding models, RAG architectures, or LLM integration patterns — signals imminent feature launch",
            f"Senior AI hires from competitors or big tech joining {company} — indicates serious commitment vs exploratory investment",
        ],
        "cloud": [
            f"{company} posting cloud partnership roles (AWS/GCP/Azure) or marketplace integration engineers — signals managed service launch is approaching",
            f"New SaaS pricing page or consumption-based billing mentions in {company}'s marketing materials — indicates cloud-native pivot",
            f"{company} hiring site reliability or platform engineers in new regions — geographic cloud expansion underway",
        ],
        "infra": [
            f"{company} hiring database internals or systems engineers with distributed systems expertise — suggests core engine rewrite or performance overhaul",
            f"Open-source contributions or benchmark publications from {company}'s engineering team — precursor to major version release",
            f"Staff/Principal engineer concentration at {company} increasing — indicates architectural-level investment, not feature work",
        ],
        "gtm": [
            f"{company} posting industry-specific sales roles (healthcare, finance, government) — signals vertical expansion strategy",
            f"New partner channel or alliances roles at {company} — indicates shift from direct sales to ecosystem-driven growth",
            f"{company} hiring competitive intelligence or product marketing roles — suggests positioning shift or new market entry",
        ],
        "data_eng": [
            f"{company} posting connector or integration engineer roles for new platforms — directly expands their competitive surface against Actian",
            f"New data pipeline or transformation-focused roles at {company} beyond their core product — signals adjacency expansion",
            f"{company} hiring solutions engineers or field engineers specializing in migration — indicates they're targeting incumbent displacement",
        ],
        "security": [
            f"{company} posting compliance or audit-focused roles (SOC2, FedRAMP, HIPAA) — signals push into regulated verticals where Actian competes",
            f"New security product manager or security engineer roles at {company} — indicates standalone compliance feature, not just table stakes",
            f"{company} pursuing government or financial services certifications — would open enterprise segments where Actian has existing presence",
        ],
    }
    indicators = watch_map.get(primary_key, [
        f"Track {company}'s engineering blog and conference talks for product direction signals",
        f"Monitor job posting velocity changes at {company} — acceleration indicates strategic investment",
        f"Watch for {company} executive hires that signal new business unit formation",
    ])
    watch_for = indicators[0]  # Primary indicator for the roadmap tile

    return {
        "direction": direction,
        "rationale": rationale,
        "confidence": confidence,
        "timeline":   timeline,
        "watch_for":  watch_for,
    }


def generate_signals(enriched_rows: list[dict]) -> list[dict]:
    """Generate strategic signals for each company with 3+ postings."""
    by_company: dict[str, list[dict]] = defaultdict(list)
    for row in enriched_rows:
        company = row.get("Company", "")
        if company:
            by_company[company].append(row)

    signals = []
    eligible = {c: rows for c, rows in by_company.items() if len(rows) >= 3}
    ineligible = {c: rows for c, rows in by_company.items() if len(rows) < 3}
    print(f"\n[SIGNALS] Generating strategic signals for {len(eligible)} companies (3+ postings each)")
    if ineligible:
        print(f"[SIGNALS] {len(ineligible)} companies with <3 postings will use rule-based signal: {', '.join(sorted(ineligible))}")

    for i, (company, rows) in enumerate(sorted(eligible.items())):
        print(f"  [{i+1}/{len(eligible)}] Analyzing {company} ({len(rows)} postings)...")

        job_summaries = []
        for r in rows[:50]:
            job_summaries.append(
                f"- {r.get('Job Title', '')} | Function: {r.get('Function', 'Unknown')} | "
                f"Product Focus: {r.get('Product_Focus', 'Unknown')} | "
                f"Seniority: {r.get('Seniority', 'Unknown')} | "
                f"Location: {r.get('Location', '')}"
            )

        company_group = rows[0].get("Company_Group", "Other")
        context = "\n".join(job_summaries)

        prompt = f"""Analyze {len(rows)} job postings from {company} (segment: {company_group}).

Actian context: Enterprise data integration & analytics platform. Core strengths: hybrid data integration, real-time analytics, legacy modernization, embedded analytics. Upcoming: vector/AI product launch. Competes on data management, integration, and analytics.

Job postings:
{context}

Produce analyst-grade competitive intelligence. For each field, reason from the COMBINATION of signals — role clusters, seniority patterns, function ratios, geographic spread — not individual postings.

Return a JSON object with these fields:

- "company" (string): "{company}"
- "signal_summary" (string): 1 sharp sentence — the single most important strategic inference. Not a description of hiring, but what it MEANS for the market.
- "implications" (array of 5 strings): Each must be a specific, reasoned inference that connects a hiring pattern to a strategic move. Format: "[What they're doing] — [why it matters for Actian]". Example: "Clustering 4 senior ML engineers + 2 product managers in SF suggests an AI feature team forming around a new product line — if they ship an AI-powered data quality tool, it directly threatens Actian's integration reliability story."
- "watch_for" (array of 3 strings): Specific, observable leading indicators that would confirm or deny the inferred strategy. These must be things we can actually track: job posting changes, product announcements, conference talks, partnership signals, pricing moves, geographic expansion patterns. Each should clearly state WHAT to watch and WHAT it would indicate.
- "recommended_actions" (array of 3 strings): Non-generic, strategic actions for Actian. Each must specify: (a) action type — product (build/partner/deprioritize/differentiate), GTM (target segment/messaging/sales focus), or timing (immediate/next quarter/monitor), (b) the specific action, and (c) WHY — tied to this competitor's inferred direction. Never use "monitor closely" or "invest in innovation".
- "hiring_intensity" (string): "low" / "medium" / "high" based on volume and seniority concentration
- "dominant_function" (string): the function with most postings
- "dominant_product_focus" (string): the product area with most postings
- "threat_level" (string): "low" / "medium" / "high" / "critical" — assessed relative to Actian's specific product areas and market position
- "last_updated" (string): "{date.today().isoformat()}"

Return ONLY the JSON object, no markdown fences or other text."""

        # Call DeepSeek for LLM-grade signal generation; fall back to rule-based on failure
        raw = _call_deepseek(
            "You are a competitive intelligence analyst. Return ONLY the JSON object described, no markdown fences.",
            prompt,
            max_tokens=1500,
        )
        parsed = None
        if raw:
            text = raw.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                print(f"    [WARN] DeepSeek JSON parse error for {company} — using fallback")
        if parsed:
            # Ensure required keys are present; fill from fallback if missing
            fallback = _fallback_signal(company, company_group, rows)
            for key in ("company", "hiring_intensity", "dominant_function",
                        "dominant_product_focus", "threat_level", "last_updated"):
                if key not in parsed or not parsed[key]:
                    parsed[key] = fallback[key]
            parsed.setdefault("signal_summary", fallback.get("signal_summary", ""))
            parsed.setdefault("implications", fallback.get("implications", []))
            parsed.setdefault("watch_for", fallback.get("watch_for", []))
            parsed.setdefault("recommended_actions", fallback.get("recommended_actions", []))
            signals.append(parsed)
        else:
            signals.append(_fallback_signal(company, company_group, rows))

    # Include companies with <3 postings using rule-based signal so all tracked
    # companies appear in Market Pulse metrics (consistent universe with page 1)
    for company, rows in sorted(ineligible.items()):
        company_group = rows[0].get("Company_Group", "Other")
        signals.append(_fallback_signal(company, company_group, rows))

    return signals


def _fallback_signal(company: str, company_group: str, rows: list[dict]) -> dict:
    """Generate detailed strategic signal without API. Analyzes actual job titles."""
    functions = defaultdict(int)
    products = defaultdict(int)
    seniority_counts = defaultdict(int)
    locations = defaultdict(int)
    all_titles = []

    for r in rows:
        functions[r.get("Function", "Engineering")] += 1
        products[r.get("Product_Focus", "Platform / Infra")] += 1
        seniority_counts[r.get("Seniority", "Mid")] += 1
        loc = r.get("Location", "")
        if loc:
            locations[loc] += 1
        all_titles.append(r.get("Job Title", "").lower())

    dom_fn = max(functions, key=functions.get) if functions else "Engineering"
    dom_pf = max(products, key=products.get) if products else "Platform / Infra"

    # Never allow "Other" or "Unknown" to leak into signals
    if dom_fn in ("Other", "Unknown", ""):
        dom_fn = "Engineering"
    if dom_pf in ("Other", "Unknown", ""):
        # Pick the second-most-common product focus if available
        valid_products = {k: v for k, v in products.items() if k not in ("Other", "Unknown", "")}
        if valid_products:
            dom_pf = max(valid_products, key=valid_products.get)
        else:
            dom_pf = "Platform / Infra"

    n = len(rows)
    senior_plus = (seniority_counts.get("Senior", 0) +
                   seniority_counts.get("Director+", 0) +
                   seniority_counts.get("Principal/Staff", 0))
    senior_ratio = senior_plus / max(n, 1)

    if n >= 20:
        intensity = "high"
    elif n >= 8:
        intensity = "medium"
    else:
        intensity = "low"

    # ── Analyze title patterns for specific product/feature signals ─────
    title_blob = " ".join(all_titles)

    ai_count = sum(1 for t in all_titles if re.search(r'ai|ml|llm|genai|gen ai|machine learn', t))
    cloud_count = sum(1 for t in all_titles if re.search(r'cloud|aws|gcp|azure|kubernetes', t))
    security_count = sum(1 for t in all_titles if re.search(r'security|compliance|privacy', t))
    data_eng_count = sum(1 for t in all_titles if re.search(r'data engineer|etl|pipeline|integrat', t))
    sales_count = sum(1 for t in all_titles if re.search(r'sales|account|revenue|business dev', t))
    go_to_market = sum(1 for t in all_titles if re.search(r'sales|market|growth|demand|partner', t))
    infra_count = sum(1 for t in all_titles if re.search(r'infra|platform|sre|devops|reliab', t))

    # ── Build specific implications based on actual patterns ────────────
    implications = []

    # AI/ML signal
    if ai_count >= 2:
        pct = round(ai_count / n * 100)
        implications.append(
            f"{ai_count} AI/ML roles ({pct}% of postings) — likely building AI-powered features "
            f"such as intelligent data matching, automated schema mapping, or LLM-based query optimization"
        )

    # Cloud expansion
    if cloud_count >= 2:
        clouds = []
        if "aws" in title_blob: clouds.append("AWS")
        if "gcp" in title_blob or "google cloud" in title_blob: clouds.append("GCP")
        if "azure" in title_blob: clouds.append("Azure")
        cloud_str = ", ".join(clouds) if clouds else "multi-cloud"
        implications.append(
            f"Cloud infrastructure hiring ({cloud_count} roles) suggests expanding {cloud_str} deployment options "
            f"— potential new managed service or marketplace listing"
        )

    # Security push
    if security_count >= 2:
        implications.append(
            f"Security/compliance hiring ({security_count} roles) indicates upcoming enterprise compliance "
            f"certifications (SOC2, FedRAMP, HIPAA) or a new security product layer"
        )

    # Data engineering / ETL
    if data_eng_count >= 2:
        implications.append(
            f"Data engineering focus ({data_eng_count} roles) — likely expanding connector ecosystem, "
            f"improving data pipeline performance, or building new integration capabilities — "
            f"direct competitive overlap with Actian's data integration offering"
        )

    # Sales/GTM push
    if go_to_market >= 3:
        top_locations = sorted(locations.items(), key=lambda x: -x[1])[:3]
        loc_str = ", ".join(f"{loc}" for loc, _ in top_locations if loc)
        implications.append(
            f"Aggressive go-to-market expansion ({go_to_market} sales/marketing roles) "
            f"{'in ' + loc_str if loc_str else ''} — likely preparing for a major product launch "
            f"or entering new market segments"
        )

    # Infrastructure scaling
    if infra_count >= 3:
        implications.append(
            f"Heavy infrastructure hiring ({infra_count} roles) signals scaling for enterprise "
            f"workloads — possibly preparing for 10x traffic growth or new deployment model"
        )

    # Seniority analysis
    if senior_ratio > 0.5:
        implications.append(
            f"High senior-to-total ratio ({round(senior_ratio*100)}%) indicates strategic buildout "
            f"phase — assembling leadership team for a new product line or major platform rewrite"
        )
    elif seniority_counts.get("Entry", 0) + seniority_counts.get("Mid", 0) > n * 0.7:
        implications.append(
            f"Majority mid/entry-level hiring suggests scaling existing product execution "
            f"rather than strategic pivots — focus on feature velocity and customer onboarding"
        )

    # Threat assessment based on company group and product overlap
    direct_threat_groups = {"ETL/Connectors", "Data Intelligence", "Data Observability"}
    adjacent_threat = {"Warehouse/Processing", "Monitoring/Platforms"}

    if company_group in direct_threat_groups:
        if dom_pf in ("ETL/Integration", "Data Governance", "Data Quality"):
            implications.append(
                f"DIRECT THREAT: {company} ({company_group}) is investing in {dom_pf} — "
                f"this is core Actian territory. Monitor for product announcements, "
                f"pricing changes, and customer wins in overlapping accounts"
            )
        else:
            implications.append(
                f"{company} is a direct competitor in {company_group} but current hiring "
                f"focuses on {dom_pf} — monitor for lateral expansion into Actian's core areas"
            )
    elif company_group in adjacent_threat:
        implications.append(
            f"{company} ({company_group}) could expand into data integration "
            f"through {dom_pf} capabilities — watch for bundling or platform plays "
            f"that undercut standalone integration tools like Actian"
        )
    else:
        implications.append(
            f"{company} operates in {company_group} — indirect competitive pressure "
            f"but potential partnership opportunity for Actian's integration platform"
        )

    # ── Company-specific insights ────────────────────────────────────────
    # Build nuanced fillers based on actual data patterns
    specific_insights = []

    # Location diversity signal
    if len(locations) > 5:
        specific_insights.append(
            f"Distributed hiring across {len(locations)} locations ({', '.join(list(locations.keys())[:3])}+) "
            f"indicates global expansion or multi-hub strategy — suggests new regional offices or remote-first shift"
        )

    # Dominant function signal
    top_fn = max(functions.items(), key=lambda x: x[1])[0] if functions else "Unknown"
    if top_fn != dom_fn:
        fn_pct = round(functions.get(top_fn, 0) / n * 100)
        specific_insights.append(
            f"{top_fn} is the dominant hiring function ({fn_pct}% of roles), not {dom_fn} — "
            f"suggests core product buildout in that area, with secondary support in {dom_fn}"
        )

    # Product focus diversity
    if len(products) > 2:
        specific_insights.append(
            f"Hiring across {len(products)} product areas ({', '.join(list(products.keys())[:2])}...) "
            f"suggests portfolio expansion or major platform consolidation"
        )

    # Hiring intensity + growth trajectory
    if intensity == "high" and n >= 15:
        specific_insights.append(
            f"Aggressive {n}-role hiring surge indicates rapid product iteration or "
            f"preparation for a major launch window — expect feature announcements within 3-6 months"
        )
    elif intensity == "low" and n < 4:
        specific_insights.append(
            f"Minimal hiring ({n} roles) suggests maintenance mode or resource constraints "
            f"— potential opportunity window if Actian can capture mindshare with faster innovation"
        )

    # Technical depth signal
    technical_roles = sum(1 for t in all_titles
                         if re.search(r'engineer|architect|data|developer|scientist|research', t))
    if technical_roles > n * 0.7:
        specific_insights.append(
            f"Heavy technical hiring ({technical_roles}/{n} roles are engineering-heavy) "
            f"— indicates R&D investment rather than GTM expansion"
        )

    # Year-over-year hiring pattern (if we have data)
    specific_insights.append(
        f"Monitor {company}'s quarterly earnings calls and product roadmaps for announcements "
        f"aligned with their {top_fn.lower()} hiring surge"
    )

    # Pad implications with specific insights instead of generic fillers
    filler_idx = 0
    while len(implications) < 6 and filler_idx < len(specific_insights):
        candidate = specific_insights[filler_idx]
        # Avoid near-duplicate
        if not any(candidate[:40] in imp for imp in implications):
            implications.append(candidate)
        filler_idx += 1

    implications = implications[:6]

    # ── Summary ────────────────────────────────────────────────────────
    # Build a specific summary based on the strongest signal
    if ai_count >= 3 and ai_count / n > 0.3:
        summary = (f"{company} is making a significant AI/ML investment with {ai_count} "
                   f"AI-related roles — likely preparing to launch AI-powered features "
                   f"that could redefine their {company_group} offering.")
    elif data_eng_count >= 3:
        summary = (f"{company} is doubling down on data engineering with {data_eng_count} "
                   f"pipeline/integration roles — direct competitive move against "
                   f"Actian in the data integration space.")
    elif go_to_market >= 4:
        summary = (f"{company} is in aggressive GTM expansion mode with {go_to_market} "
                   f"sales and marketing hires — likely preparing a major product push "
                   f"or entering new enterprise segments.")
    elif infra_count >= 3:
        summary = (f"{company} is scaling infrastructure with {infra_count} platform roles "
                   f"— indicates preparation for enterprise-grade deployments or "
                   f"a new managed service offering.")
    elif senior_ratio > 0.5:
        summary = (f"{company} is assembling senior leadership ({round(senior_ratio*100)}% "
                   f"senior+ roles) — signals a strategic pivot or new product line launch "
                   f"in {dom_pf}.")
    else:
        summary = (f"{company} is actively hiring {n} roles across {dom_fn} focused on "
                   f"{dom_pf}, with {intensity} intensity — steady investment in their "
                   f"{company_group} capabilities.")

    # ── Threat level ──────────────────────────────────────────────────
    threat_score = 0
    if company_group in direct_threat_groups:
        threat_score += 3
    elif company_group == "Vector DB / AI":
        threat_score += 3  # Strategic priority — Actian's upcoming vector product
    elif company_group in adjacent_threat:
        threat_score += 1
    if dom_pf in ("ETL/Integration", "Data Governance", "Data Quality", "Vector / AI", "Vector / Embedding"):
        threat_score += 3
    if company_group == "Vector DB / AI":
        threat_score += 2  # Extra boost — direct future competitor
    if intensity == "high":
        threat_score += 2
    elif intensity == "medium":
        threat_score += 1
    if senior_ratio > 0.4:
        threat_score += 1
    if ai_count >= 2:
        threat_score += 1

    # ── Scale threat by posting volume magnitude ──────────────────────
    # Large hiring volumes are inherently threatening regardless of category
    if n >= 150:
        threat_score += 3  # Very high volume — strategic commitment
    elif n >= 100:
        threat_score += 2  # High volume

    # ── Final threat level assignment ─────────────────────────────────
    # Volume threshold: 150+ jobs is automatically CRITICAL
    if n >= 150 and threat_score >= 5:
        threat = "critical"
    elif threat_score >= 7:
        threat = "critical"
    elif threat_score >= 5:
        threat = "high"
    elif threat_score >= 3:
        threat = "medium"
    else:
        threat = "low"

    roadmap = _infer_roadmap(
        company=company, company_group=company_group, n=n, all_titles=all_titles,
        ai_count=ai_count, cloud_count=cloud_count, data_eng_count=data_eng_count,
        go_to_market=go_to_market, infra_count=infra_count, security_count=security_count,
        senior_ratio=senior_ratio, dom_pf=dom_pf, dom_fn=dom_fn,
    )

    # ── Watch For: specific leading indicators tailored to company ────
    watch_for_items = []

    if company_group in direct_threat_groups:
        if data_eng_count >= 2:
            watch_for_items.append(
                f"{company} posting new connector or integration-specific roles — each new connector "
                f"directly expands their overlap with Actian's integration catalog"
            )
        if go_to_market >= 3:
            watch_for_items.append(
                f"Uptick in {company}'s enterprise AE or solution architect hiring in Actian's "
                f"core verticals (manufacturing, financial services) — signals direct competitive displacement effort"
            )
        watch_for_items.append(
            f"{company} announcing partnerships with cloud data platforms (Snowflake, Databricks) — "
            f"would signal an embedded integration play that threatens Actian's standalone value proposition"
        )
        if senior_ratio > 0.4:
            watch_for_items.append(
                f"{company}'s senior hiring concentration ({round(senior_ratio*100)}%) shifting toward "
                f"product or architecture roles — indicates new product line formation, not just scaling"
            )
        watch_for_items.append(
            f"{company} hiring in new geographies or verticals beyond their current footprint — "
            f"geographic expansion in {company_group} typically precedes competitive displacement campaigns "
            f"against incumbents like Actian"
        )
    elif company_group == "Vector DB / AI":
        watch_for_items.append(
            f"{company} releasing enterprise features (RBAC, audit logs, SOC2) — signals readiness "
            f"to compete for the same enterprise buyers Actian targets with its upcoming vector product"
        )
        watch_for_items.append(
            f"{company} hiring data integration or ETL engineers — would indicate expansion from "
            f"vector search into data pipeline territory, creating direct Actian overlap"
        )
        watch_for_items.append(
            f"Developer community growth metrics for {company} (GitHub stars, Discord members, "
            f"conference presence) — leading indicator of mindshare capture before Actian's vector launch"
        )
    elif company_group in adjacent_threat:
        watch_for_items.append(
            f"{company} adding integration or connector capabilities to their {company_group} platform — "
            f"bundled integration would undercut Actian's standalone pricing"
        )
        watch_for_items.append(
            f"{company} hiring product managers focused on data movement or pipeline orchestration — "
            f"signals adjacency expansion into Actian's core territory"
        )
        watch_for_items.append(
            f"Changes in {company}'s pricing model (consumption-based, freemium tiers) — "
            f"aggressive pricing in adjacent space pressures Actian's value narrative"
        )
    else:
        watch_for_items.append(
            f"Track {company}'s job posting velocity — sustained increase (>20% quarter-over-quarter) "
            f"signals strategic investment acceleration, not just backfill"
        )
        watch_for_items.append(
            f"{company} hiring product roles focused on data integration or analytics — "
            f"would signal expansion into Actian's competitive territory"
        )
        watch_for_items.append(
            f"{company} executive hires from data infrastructure companies — "
            f"leadership moves telegraph strategic direction 6-12 months before product announcements"
        )

    if ai_count >= 2 and not any("AI" in w or "vector" in w for w in watch_for_items):
        watch_for_items.insert(0,
            f"{company}'s AI team growing past {ai_count} roles — crossing 5+ AI hires typically "
            f"precedes a standalone AI feature launch within 2 quarters"
        )

    # Ensure minimum 3 indicators — add context-specific fillers if needed
    _extra_indicators = [
        f"{company}'s job posting velocity changing quarter-over-quarter — sustained "
        f"acceleration (>20%) in {dom_fn} roles signals strategic investment, not just backfill",
        f"Executive or VP-level hires at {company} from data infrastructure companies — "
        f"leadership moves telegraph strategic direction 6-12 months before product shifts",
        f"{company} increasing presence at data engineering conferences (dbt Coalesce, "
        f"Data Council, Subsurface) — conference strategy reveals go-to-market timing",
    ]
    for extra in _extra_indicators:
        if len(watch_for_items) >= 3:
            break
        if not any(extra[:40] in w for w in watch_for_items):
            watch_for_items.append(extra)

    watch_for_items = watch_for_items[:3]

    # ── Recommended Actions: strategic, non-generic, tied to this competitor ──
    rec_actions = []

    if threat in ("critical", "high"):
        if dom_pf in ("ETL/Integration", "Data Quality", "Data Governance"):
            rec_actions.append(
                f"[Immediate / GTM] Pull recent win/loss data against {company} in {dom_pf} deals — "
                f"identify the 3 accounts most at risk and arm AEs with differentiated positioning "
                f"around Actian's hybrid deployment and legacy system connectivity"
            )
        else:
            rec_actions.append(
                f"[Immediate / GTM] Brief enterprise AEs on {company}'s trajectory in {dom_pf} — "
                f"their {n}-role hiring surge signals aggressive expansion that will hit Actian's "
                f"pipeline within 1-2 quarters"
            )
    else:
        rec_actions.append(
            f"[Next Quarter / Product] Evaluate {company}'s {dom_pf} trajectory against Actian's "
            f"roadmap — determine if Actian needs to build competitive parity or can differentiate "
            f"by doubling down on integration depth"
        )

    if company_group == "Vector DB / AI":
        rec_actions.append(
            f"[Immediate / Product] Accelerate Actian's vector/AI launch to establish enterprise "
            f"positioning before {company} captures developer and buyer mindshare — {company}'s "
            f"{n} active roles suggest they're building production-grade capabilities now"
        )
    elif ai_count >= 2:
        rec_actions.append(
            f"[Next Quarter / Product] Assess whether {company}'s AI investment (currently {ai_count} "
            f"roles) threatens Actian's analytics value proposition — if they ship AI-powered {dom_pf}, "
            f"Actian needs to counter with AI capabilities in the integration layer"
        )
    elif go_to_market >= 3:
        rec_actions.append(
            f"[Immediate / GTM] Preemptively engage {company}'s target accounts — their "
            f"{go_to_market} GTM hires signal a land-grab in progress. Focus on demonstrating "
            f"Actian's time-to-value advantage in competitive evaluations"
        )
    else:
        rec_actions.append(
            f"[Next Quarter / Product] Map {company}'s {dom_pf} capability evolution against "
            f"Actian's integration platform — identify where {company}'s feature expansion could "
            f"make them a viable substitute for Actian in mid-market deals"
        )

    if intensity == "high":
        rec_actions.append(
            f"[Immediate / GTM] Add {company} to Actian's top-5 competitive watchlist — "
            f"their {n}-role hiring velocity combined with {dom_fn} concentration "
            f"indicates a product launch cycle. Prepare competitive battle cards by end of quarter"
        )
    elif senior_ratio > 0.5:
        rec_actions.append(
            f"[Monitor / Product] {company}'s senior-heavy hiring ({round(senior_ratio*100)}%) "
            f"suggests an architectural rebuild — the resulting product will likely be significantly "
            f"different from their current offering. Re-assess competitive positioning when they ship"
        )
    else:
        rec_actions.append(
            f"[Next Quarter / GTM] Include {company}'s {company_group} trajectory in Actian's "
            f"quarterly competitive review — share {dom_pf} hiring patterns with product leadership "
            f"to inform roadmap prioritization decisions"
        )

    rec_actions = rec_actions[:3]

    return {
        "company": company,
        "company_group": company_group,
        "posting_count": n,
        "signal_summary": summary,
        "implications": implications,
        "watch_for": watch_for_items,
        "recommended_actions": rec_actions,
        "hiring_intensity": intensity,
        "dominant_function": dom_fn,
        "dominant_product_focus": dom_pf,
        "threat_level": threat,
        "roadmap": roadmap,
        "last_updated": date.today().isoformat(),
    }

# ══════════════════════════════════════════════════════════════════════════
# MAIN ENRICHMENT PIPELINE
# ══════════════════════════════════════════════════════════════════════════

def load_existing(path: str) -> dict:
    existing = {}
    if not os.path.exists(path):
        return existing
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            link = row.get("Job Link", "")
            if link:
                existing[link] = row
    return existing


def enrich(
    raw_path: str = "data/jobs_raw.csv",
    enriched_path: str = "data/jobs_enriched.csv",
    signals_path: str = "data/signals.json",
    competitors_path: str = "competitors.csv",
):
    _load_csv_groups(competitors_path)

    existing = load_existing(enriched_path)
    print(f"[ENRICH] Loaded {len(existing)} existing enriched rows")

    raw_rows = []
    if not os.path.exists(raw_path):
        print(f"[ERROR] {raw_path} not found")
        return 0
    with open(raw_path, encoding="utf-8") as f:
        raw_rows = list(csv.DictReader(f))
    print(f"[ENRICH] {len(raw_rows)} raw rows to process")

    today = date.today().isoformat()
    cutoff_date = (date.today() - timedelta(days=MAX_JOB_AGE_DAYS)).isoformat()

    new_rows = []
    carry_rows = []
    reclassify_rows = []  # Carried rows with "Other" that need re-classification
    dropped_old = 0

    for row in raw_rows:
        link = row.get("Job Link", "")
        if link in existing:
            ex = existing[link]

            # Drop jobs older than MAX_JOB_AGE_DAYS
            posting_date = ex.get("Posting Date", "")
            if posting_date and posting_date < cutoff_date:
                dropped_old += 1
                continue

            ex["Last_Seen"] = today

            # Backfill posting date from raw row if previously missing
            if not posting_date:
                raw_date = row.get("Posting Date", "")
                if raw_date:
                    ex["Posting Date"] = raw_date
                    posting_date = raw_date

            if posting_date:
                try:
                    d = datetime.fromisoformat(posting_date).date()
                    ex["Days Since Posted"] = str((date.today() - d).days)
                except Exception:
                    pass

            # Re-classify carried rows that have "Other" or "Unknown" in key fields
            pf = ex.get("Product_Focus", "")
            fn = ex.get("Function", "")
            sen = ex.get("Seniority", "")
            needs_reclassify = (
                not pf or pf not in ALLOWED_PRODUCT_FOCUS
                or not fn or fn not in ALLOWED_FUNCTIONS
                or sen in ("Unknown", "")
            )
            if needs_reclassify:
                reclassify_rows.append(ex)
            else:
                # Refresh skills for carried rows that have empty skills
                existing_skills_raw = ex.get("Extracted_Skills", "[]")
                try:
                    existing_skills = json.loads(existing_skills_raw)
                except Exception:
                    existing_skills = []
                if not existing_skills:
                    title = ex.get("Job Title", "")
                    skills = extract_skills(title)
                    ex["Primary_Skill"] = skills[0] if skills else ""
                    ex["Extracted_Skills"] = json.dumps(skills)
                    ex["Trend_Score"] = compute_trend(title, sen)
                else:
                    skills = existing_skills
                # Always recompute relevancy so the cap (min 10.0) is applied to old rows
                ex["Relevancy_to_Actian"] = compute_relevancy(skills, ex.get("Location", ""), pf, sen, ex.get("Company_Group", ""))
                carry_rows.append(ex)
        else:
            new_rows.append(row)

    if dropped_old:
        print(f"[ENRICH] Dropped {dropped_old} jobs older than {MAX_JOB_AGE_DAYS} days")
    print(f"[ENRICH] {len(new_rows)} new rows to classify, {len(carry_rows)} carried forward, {len(reclassify_rows)} to re-classify")

    classified = []
    BATCH = 20
    for i in range(0, len(new_rows), BATCH):
        batch = new_rows[i:i + BATCH]
        titles = [r.get("Job Title", "") for r in batch]
        batch_num = i // BATCH + 1
        total_batches = max(1, (len(new_rows) - 1) // BATCH + 1)
        print(f"  Classifying batch {batch_num}/{total_batches} ({len(titles)} titles)...")

        descriptions_batch = [r.get("Description", "") for r in batch]
        results = classify_batch(titles, descriptions_batch)
        time.sleep(0.5)

        for row, cls in zip(batch, results):
            skills = extract_skills(row.get("Job Title", ""))

            # Re-detect seniority with improved logic
            seniority = detect_seniority(row.get("Job Title", ""))

            pf = cls.get("product_focus", "Platform / Infra")
            fn = cls.get("function", "Engineering")
            location = row.get("Location", "")
            company_group = classify_company_group(row.get("Company", ""))

            posting_date = row.get("Posting Date", "")
            days_since = ""
            if posting_date:
                try:
                    d = datetime.fromisoformat(posting_date).date()
                    days_since = str((date.today() - d).days)
                except Exception:
                    pass

            enriched_row = {
                "Company":             row.get("Company", ""),
                "Job Title":           row.get("Job Title", ""),
                "Job Link":            row.get("Job Link", ""),
                "Location":            location,
                "Posting Date":        posting_date,
                "Days Since Posted":   days_since,
                "Function":            fn,
                "Seniority":           seniority,
                "Company_Group":       company_group,
                "Product_Focus":       pf,
                "Product_Focus_Tokens": json.dumps([pf]),
                "Primary_Skill":       skills[0] if skills else "",
                "Extracted_Skills":    json.dumps(skills),
                "Relevancy_to_Actian": compute_relevancy(skills, location, pf, seniority, company_group),
                "Trend_Score":         compute_trend(row.get("Job Title", ""), seniority),
                "First_Seen":          row.get("First_Seen", today),
                "Last_Seen":           today,
                "Description":         row.get("Description", ""),
            }
            classified.append(enriched_row)

    # ── Re-classify carried rows that had "Other" / "Unknown" ──────────
    if reclassify_rows:
        print(f"\n[RECLASSIFY] Re-classifying {len(reclassify_rows)} rows with invalid categories...")
        for i in range(0, len(reclassify_rows), BATCH):
            batch = reclassify_rows[i:i + BATCH]
            titles = [r.get("Job Title", "") for r in batch]
            descriptions_batch = [r.get("Description", "") for r in batch]
            results = classify_batch(titles, descriptions_batch)
            time.sleep(0.3)

            for row, cls in zip(batch, results):
                title = row.get("Job Title", "")
                pf = cls.get("product_focus", "Platform / Infra")
                fn = cls.get("function", "Engineering")

                # Update the carried row in-place
                row["Product_Focus"] = pf
                row["Product_Focus_Tokens"] = json.dumps([pf])
                row["Function"] = fn

                # Fix seniority if Unknown
                if row.get("Seniority", "") in ("Unknown", ""):
                    row["Seniority"] = detect_seniority(title)

                # Recompute scores with fixed classification
                skills = extract_skills(title)
                seniority = row.get("Seniority", "Mid")
                company_group = row.get("Company_Group", classify_company_group(row.get("Company", "")))
                location = row.get("Location", "")
                row["Primary_Skill"] = skills[0] if skills else ""
                row["Extracted_Skills"] = json.dumps(skills)
                row["Relevancy_to_Actian"] = compute_relevancy(skills, location, pf, seniority, company_group)
                row["Trend_Score"] = compute_trend(title, seniority)

    all_rows = carry_rows + reclassify_rows + classified
    dedup: dict[str, dict] = {}
    for r in all_rows:
        link = r.get("Job Link", "")
        if link not in dedup:
            dedup[link] = r

    # Remove Legal and People/HR — not relevant for competitive intelligence
    # Also remove junk jobs (product pages, downloads, etc.)
    dedup = {k: v for k, v in dedup.items()
             if v.get("Function", "") not in EXCLUDED_FUNCTIONS
             and not is_junk_job(v.get("Job Title", ""))}

    out = sorted(dedup.values(), key=lambda x: (x.get("Company", ""), x.get("Job Title", "")))

    with open(enriched_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for r in out:
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

    print(f"\n[OK] Wrote {len(out)} enriched rows → {enriched_path}")

    print(f"\n{'='*60}")
    print("[SIGNALS] Generating strategic intelligence signals...")
    print(f"{'='*60}")
    signals = generate_signals(out)

    with open(signals_path, "w", encoding="utf-8") as f:
        json.dump(signals, f, indent=2, ensure_ascii=False)

    print(f"\n[OK] Wrote {len(signals)} company signals → {signals_path}")

    print(f"\n{'='*60}")
    print(f"ENRICHMENT SUMMARY")
    print(f"  Total enriched rows:  {len(out)}")
    print(f"  New rows classified:  {len(classified)}")
    print(f"  Re-classified:        {len(reclassify_rows)}")
    print(f"  Carried forward:      {len(carry_rows)}")
    print(f"  Dropped (>365 days):  {dropped_old}")
    print(f"  Signals generated:    {len(signals)}")
    critical = sum(1 for s in signals if s.get("threat_level") == "critical")
    high = sum(1 for s in signals if s.get("threat_level") == "high")
    if critical or high:
        print(f"  CRITICAL threats:     {critical}")
        print(f"  HIGH threats:         {high}")
    print(f"{'='*60}")

    return len(out)


if __name__ == "__main__":
    enrich()
