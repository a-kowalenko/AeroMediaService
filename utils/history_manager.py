import json
import os
import uuid
from datetime import datetime

class HistoryManager:
    def __init__(self, file_path="upload_history.json"):
        self.file_path = file_path
        self.history = self.load_history()

    def load_history(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def save_history(self):
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.history, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Failed to save history: {e}")

    def add_or_update(self, data):
        # data needs "dir_name" to identify
        dir_name = data.get("dir_name")
        if not dir_name:
            return

        # Find existing entry by dir_name
        existing = next((item for item in self.history if item.get("dir_name") == dir_name), None)

        if existing:
            existing.update(data)
            existing["last_updated"] = datetime.now().isoformat()
        else:
            new_entry = {
                "id": str(uuid.uuid4()),
                "dir_name": dir_name,
                "status": data.get("status", "Gestartet"),
                "email_status": data.get("email_status", ""),
                "sms_status": data.get("sms_status", ""),
                "error_msg": data.get("error_msg", ""),
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat()
            }
            new_entry.update(data)
            self.history.insert(0, new_entry)  # newest first

        self.save_history()

    def delete_items(self, ids):
        self.history = [item for item in self.history if item.get("id") not in ids]
        self.save_history()

    def clear_all(self):
        self.history = []
        self.save_history()

    def get_filtered_history(self, search_text=""):
        if not search_text:
            return self.history

        search_text = search_text.lower()
        filtered = []
        for item in self.history:
            # Check all values
            if any(search_text in str(v).lower() for v in item.values()):
                filtered.append(item)
        return filtered

