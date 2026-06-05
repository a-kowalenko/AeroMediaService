from dataclasses import dataclass
from typing import Optional

_INVALID_PHONE_VALUES = frozenset({"none", "null", "nan"})


def normalize_phone(value) -> Optional[str]:
    """Gibt eine bereinigte Telefonnummer zurück oder None bei fehlendem/ungültigem Wert."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in _INVALID_PHONE_VALUES:
        return None
    return s


@dataclass
class Kunde:
    customer_number: Optional[str] = None
    booking_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    type: Optional[str] = None
    handcam_foto: bool = False
    handcam_video: bool = False
    outside_foto: bool = False
    outside_video: bool = False
    ist_bezahlt_handcam_foto: bool = False
    ist_bezahlt_handcam_video: bool = False
    ist_bezahlt_outside_foto: bool = False
    ist_bezahlt_outside_video: bool = False