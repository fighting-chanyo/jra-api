import sys
from datetime import date, datetime
from app.services.race_service import RaceService

def main():
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            return
    else:
        target_date = date.today()
        print("No date specified. Using today.")

    print(f"Starting manual result update for {target_date}...")
    
    service = RaceService()
    result = service.update_results(target_date)
    
    print("Update complete.")
    print(f"Processed: {result['processed']}")
    print(f"Hits: {result['hits']}")

if __name__ == "__main__":
    main()
