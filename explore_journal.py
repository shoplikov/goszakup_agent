from src.etl.client import GoszakupClient
from datetime import datetime, timedelta
from collections import Counter
import json

def explore_journal():
    client = GoszakupClient()
    
    # Set the time window to the last 24 hours
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    date_from = yesterday.strftime('%Y-%m-%d')
    date_to = today.strftime('%Y-%m-%d')
    
    print(f"📡 Fetching CDC Journal from {date_from} to {date_to}...\n")
    
    params = {
        'date_from': date_from,
        'date_to': date_to
    }
    
    try:
        # Fetch just the FIRST page to analyze the structure
        data = client.get('/v3/journal', params=params)
        events = data.get('items', []) if isinstance(data, dict) else data
        
        if not events:
            print("⚠️ No events found for this date range.")
            return
            
        print(f"✅ Fetched first page. Found {len(events)} events on this page.")
        
        # Aggregate the service names to see what data is flowing
        services = Counter(e.get('service_name') for e in events)
        actions = Counter(e.get('action') for e in events)
        
        print("\n📊 Actions Breakdown:")
        for action, count in actions.items():
            print(f"  - {action}: {count}")

        print("\n📊 Service Name Distribution (Page 1):")
        for name, count in services.items():
            print(f"  - {name}: {count}")
            
        print("\n" + "="*50)
        
        # Isolate and print a sample 'Update' event
        print("\n👀 Sample 'Update' (U) Event:")
        updates = [e for e in events if e.get('action') == 'U']
        if updates:
            print(json.dumps(updates[0], indent=4, ensure_ascii=False))
            
        # Isolate and print a sample 'Delete' event
        print("\n👀 Sample 'Delete' (D) Event:")
        deletes = [e for e in events if e.get('action') == 'D']
        if deletes:
            print(json.dumps(deletes[0], indent=4, ensure_ascii=False))

    except Exception as e:
        print(f"❌ Error fetching journal: {e}")

if __name__ == "__main__":
    explore_journal()