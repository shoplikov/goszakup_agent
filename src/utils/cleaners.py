import re
import html
from typing import Optional

def sanitize_lot_text(text: Optional[str]) -> Optional[str]:
    """
    Removes HTML tags, unescapes HTML entities, and normalizes whitespace.
    """
    if not text:
        return text
        
    # Unescape HTML entities (e.g., &quot; -> ", &amp; -> &)
    cleaned = html.unescape(text)
    
    # Remove HTML tags (e.g., <p>, <br>, <span>)
    cleaned = re.sub(r'<[^>]+>', ' ', cleaned)
    
    # Remove weird non-breaking spaces and normalize whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Strip leading/trailing whitespace
    return cleaned.strip()