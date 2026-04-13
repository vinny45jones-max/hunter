from dataclasses import dataclass
from typing import Optional


@dataclass
class Vacancy:
    external_id: str
    url: str
    title: str
    company: Optional[str] = None
    salary: Optional[str] = None
    city: Optional[str] = None
    description: Optional[str] = None
    relevance_score: int = 0
    relevance_reason: Optional[str] = None
    cover_letter: Optional[str] = None
    status: str = "new"
    id: Optional[int] = None
    apply_state: str = "idle"  # idle | previewing | editing | sending | sent | failed
    cover_letter_version: int = 0
    employer_requirements: Optional[str] = None  # JSON: требования работодателя к письму
    require_cover_letter: bool = False
    negotiation_id: Optional[str] = None


@dataclass
class Message:
    message_id: str
    text: str
    direction: str  # 'incoming' | 'outgoing'
    sender: Optional[str] = None
    vacancy_id: Optional[int] = None
    vacancy_title: Optional[str] = None
    company: Optional[str] = None
    conversation_id: Optional[str] = None
    is_read: bool = False
    replied: bool = False
    id: Optional[int] = None


@dataclass
class Conversation:
    conversation_id: str
    vacancy_title: Optional[str] = None
    company: Optional[str] = None
    vacancy_id: Optional[int] = None
    last_message_at: Optional[str] = None
    status: str = "active"  # active, waiting_reply, replied, closed
    id: Optional[int] = None
