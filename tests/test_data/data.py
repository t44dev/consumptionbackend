from datetime import datetime

from consumptionbackend.database import (
    ConsumableFieldsRequired,
    PersonnelFieldsRequired,
    SeriesFieldsRequired,
    WhereMapping,
    WhereOperator,
    WhereQuery,
)
from consumptionbackend.entities import Status

CONSUMABLE_REQUIRED: ConsumableFieldsRequired = {
    "series_id": 4,
    "name": "Test Consumable",
    "type": "TYPE",
    "status": Status.ON_HOLD,
    "parts": 4,
    "max_parts": 44,
    "completions": 4_444,
    "rating": 4.4,
    "start_date": datetime.fromtimestamp(4_444),
}

SERIES_REQUIRED: SeriesFieldsRequired = {"name": "Test Series"}

PERSONNEL_REQUIRED: PersonnelFieldsRequired = {
    "first_name": "TestFirstName",
    "last_name": "TestLastName",
    "pseudonym": "TestPseudonym",
}

SIMPLE_WHERE: WhereMapping = {
    "consumables": {
        "name": [WhereQuery("SimpleName", WhereOperator.LIKE)],
        "completions": [WhereQuery(44, WhereOperator.GT)],
    }
}

SIMPLE_WHERE_VALUES = ["%simplename%", 44]

COMPLEX_WHERE: WhereMapping = {
    "consumables": {
        "id": [WhereQuery(4, WhereOperator.GT), WhereQuery(44, WhereOperator.LTE)],
        "series_id": [
            WhereQuery(4, WhereOperator.GTE),
            WhereQuery(44, WhereOperator.LT),
        ],
        "name": [WhereQuery("tEsT nAmE", WhereOperator.LIKE)],
        "type": [WhereQuery("TestType", WhereOperator.EQ)],
        "status": [
            WhereQuery(Status.IN_PROGRESS, WhereOperator.GTE),
            WhereQuery(Status.COMPLETED, WhereOperator.LT),
        ],
        "parts": [WhereQuery(4, WhereOperator.GT), WhereQuery(44, WhereOperator.LT)],
        "max_parts": [
            WhereQuery(4, WhereOperator.GT),
            WhereQuery(44, WhereOperator.LT),
        ],
        "completions": [
            WhereQuery(4, WhereOperator.GT),
            WhereQuery(44, WhereOperator.LT),
        ],
        "rating": [
            WhereQuery(0.4, WhereOperator.GT),
            WhereQuery(4, WhereOperator.GT),
            WhereQuery(4.4, WhereOperator.GT),
        ],
        "start_date": [
            WhereQuery(datetime.fromtimestamp(44), WhereOperator.GT),
            WhereQuery(datetime.fromtimestamp(44_444), WhereOperator.LT),
        ],
        "end_date": [
            WhereQuery(datetime.fromtimestamp(444_444), WhereOperator.EQ),
        ],
    },
    "personnel": {
        "id": [WhereQuery(4, WhereOperator.GT), WhereQuery(44, WhereOperator.LTE)],
        "first_name": [WhereQuery("tEsTfIrStNaMe", WhereOperator.LIKE)],
        "last_name": [WhereQuery("TestLastName", WhereOperator.EQ)],
        "pseudonym": [WhereQuery("tEsTPsEuDoNyM", WhereOperator.LIKE)],
        "role": [
            WhereQuery("TestRole", WhereOperator.EQ),
        ],
    },
    "series": {
        "id": [WhereQuery(4, WhereOperator.GT), WhereQuery(44, WhereOperator.LTE)],
        "name": [WhereQuery("tEsT nAmE", WhereOperator.LIKE)],
    },
}

COMPLEX_WHERE_VALUES = [
    4,
    44,
    4,
    44,
    "%test name%",
    "TestType",
    Status.IN_PROGRESS.value,
    Status.COMPLETED.value,
    4,
    44,
    4,
    44,
    4,
    44,
    0.4,
    4,
    4.4,
    44,
    44_444,
    444_444,
    4,
    44,
    "%testfirstname%",
    "TestLastName",
    "%testpseudonym%",
    "TestRole",
    4,
    44,
    "%test name%",
]
