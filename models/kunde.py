from dataclasses import dataclass
from typing import Optional

@dataclass
class Kunde:
    customer_number: Optional[int] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    handcam_foto: bool = False
    handcam_video: bool = False
    outside_foto: bool = False
    outside_video: bool = False
    ist_bezahlt_handcam_foto: bool = False
    ist_bezahlt_handcam_video: bool = False
    ist_bezahlt_outside_foto: bool = False
    ist_bezahlt_outside_video: bool = False