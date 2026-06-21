from PySide6.QtCore import Qt, QTimer, QRectF
from PySide6.QtGui import QPainter, QColor, QPalette, QPen, QFont
from PySide6.QtWidgets import QWidget


class LoadingOverlay(QWidget):
    """Halbtransparente Schicht mit zentriertem Lade-Indikator und optionalem Text."""

    def __init__(self, parent=None, *, message: str = ""):
        super().__init__(parent)
        self._angle = 0
        self._message = message
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._advance_angle)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)

    def _advance_angle(self):
        self._angle = (self._angle + 11) % 360
        self.update()

    def set_message(self, message: str):
        self._message = message or ""
        self.update()

    def show_loading(self, message: str | None = None):
        if message is not None:
            self._message = message
        self._angle = 0
        if self.parentWidget():
            self.setGeometry(self.parentWidget().rect())
        self._timer.start(40)
        self.show()
        self.raise_()

    def hide_loading(self):
        self._timer.stop()
        self.hide()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        scrim = QColor(self.palette().color(QPalette.ColorRole.Window))
        scrim.setAlpha(120)
        painter.fillRect(self.rect(), scrim)

        cx = self.width() / 2.0
        cy = self.height() / 2.0
        card_h = 80 if not self._message else 110
        card = QRectF(cx - 120, cy - card_h / 2, 240, card_h)
        base = QColor(self.palette().color(QPalette.ColorRole.Base))
        base.setAlpha(242)
        painter.setBrush(base)
        painter.setPen(QPen(self.palette().color(QPalette.ColorRole.Mid), 1))
        painter.drawRoundedRect(card, 14, 14)

        ring = QRectF(cx - 22, cy - 22 - (12 if self._message else 0), 44, 44)
        track = QPen(self.palette().color(QPalette.ColorRole.Mid))
        track.setWidth(5)
        track.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(track)
        painter.drawArc(ring, 0, 360 * 16)

        accent = QPen(self.palette().color(QPalette.ColorRole.Highlight))
        accent.setWidth(5)
        accent.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(accent)
        painter.drawArc(ring, self._angle * 16, 280 * 16)

        if self._message:
            painter.setPen(self.palette().color(QPalette.ColorRole.Text))
            font = QFont(self.font())
            font.setPointSize(max(9, font.pointSize()))
            painter.setFont(font)
            text_rect = QRectF(card.left() + 12, ring.bottom() + 8, card.width() - 24, 36)
            painter.drawText(text_rect, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop, self._message)
