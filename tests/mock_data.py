from datetime import datetime, timezone

# Realistic items for Dict and AST parsers
MOCK_ITEMS = [
    {
        "id": "uuid-1",
        "name": "Project Alpha",
        "type": "INTERNAL",
        "status": "active",
        "priority": 1,
        "tags": ["urgent", "core"],
        "metadata": {
            "version": "1.2.0",
            "owner": "Alice",
            "settings": {"notifications": True, "retention_days": 30}
        },
        "metrics": {"score": 95.5, "coverage": 88},
        "created_at": "2023-01-15",
        "last_updated": "10:30:00",
        "history": [
            {"date": "2023-01-10", "event": "created"},
            {"date": "2023-01-15", "event": "updated"}
        ]
    },
    {
        "id": "uuid-2",
        "name": "Beta Integration",
        "type": "EXTERNAL",
        "status": "pending",
        "priority": 2,
        "tags": ["integration", "beta"],
        "metadata": {
            "version": "0.9.5",
            "owner": "Bob",
            "settings": {"notifications": False, "retention_days": 7}
        },
        "metrics": {"score": 82.0, "coverage": 75},
        "created_at": "2023-05-20",
        "last_updated": "14:45:00",
        "history": [
            {"date": "2023-05-20", "event": "created"}
        ]
    },
    {
        "id": "uuid-3",
        "name": "Gamma Service",
        "type": "INTERNAL",
        "status": "archived",
        "priority": 3,
        "tags": ["legacy"],
        "metadata": None,
        "metrics": {"score": 45.0, "coverage": 60},
        "created_at": "2022-11-01",
        "last_updated": "09:00:00",
        "history": []
    },
    {
        "id": "uuid-4",
        "name": "Delta API",
        "type": "EXTERNAL",
        "status": "active",
        "priority": 1,
        "tags": ["api", "public"],
        "metadata": {
            "version": "2.0.1",
            "owner": "Alice",
            "settings": {"notifications": True, "retention_days": 365}
        },
        "metrics": {"score": 99.9, "coverage": 100},
        "created_at": "2023-08-10",
        "last_updated": "18:00:00",
        "history": [
            {"date": "2023-08-01", "event": "alpha"},
            {"date": "2023-08-10", "event": "release"}
        ]
    }
]

# Mock MongoDB documents (similar to MOCK_ITEMS but potentially with Mongo-specific types if needed)
MOCK_MONGO_DOCS = [
    {
        "_id": "507f1f77bcf86cd799439011",
        "call_id": "call_100",
        "duration": 45,
        "direction": "inbound",
        "timestamp": datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    },
    {
        "_id": "507f1f77bcf86cd799439012",
        "call_id": "call_200",
        "duration": 120,
        "direction": "outbound",
        "timestamp": datetime(2023, 10, 1, 13, 0, 0, tzinfo=timezone.utc)
    },
    {
        "_id": "507f1f77bcf86cd799439013",
        "call_id": "call_300",
        "duration": 15,
        "direction": "inbound",
        "timestamp": datetime(2023, 10, 1, 14, 0, 0, tzinfo=timezone.utc)
    }
]
