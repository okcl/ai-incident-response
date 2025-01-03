import os
import json
import spacy
import re
from arcgis.gis import GIS
from arcgis.geocoding import geocode
from dotenv import load_dotenv

class DataProcessor:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        self.raw_data_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../data/raw")
        )
        self.processed_data_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../data/processed")
        )
        os.makedirs(self.processed_data_path, exist_ok=True)

        # Load environment variables
        load_dotenv()
        self.arcgis_api_key = os.getenv("ARCGIS_API_KEY")

        if not self.arcgis_api_key:
            raise ValueError("API key is missing. Set ARCGIS_API_KEY in the .env file.")

        # Initialize GIS with API key
        try:
            self.gis = GIS("https://www.arcgis.com", api_key=self.arcgis_api_key)
            print("Logged in using API key.")
        except Exception as e:
            print("GIS initialization failed:", e)
            raise ValueError("Invalid API key or connection issue.")

        # Load NLP model
        self.nlp = spacy.load("en_core_web_sm")

    def extract_location(self, text):
        """
        Extract city and country using NLP and geocode for coordinates.
        """
        doc = self.nlp(text)
        for ent in doc.ents:
            if ent.label_ in ["GPE", "LOC"]:  # Geopolitical Entity or Location
                result = geocode(ent.text, max_locations=1, as_featureset=True)
                if result and result.sdf.shape[0] > 0:
                    geocoded = result.sdf.iloc[0]
                    return {
                        "city": geocoded.get("City", ""),
                        "country": geocoded.get("Country", ""),
                        "coordinates": [geocoded.get("Y"), geocoded.get("X")]
                    }
        return {"city": "", "country": "", "coordinates": []}

    def identify_incident_type(self, text):
        """
        Identify incident type using spaCy Named Entity Recognition (NER).
        """
        doc = self.nlp(text)
        incident_keywords = ["flood", "earthquake", "fire", "crash", "storm", "hurricane", "tornado"]

        # Check for entities in the text
        for ent in doc.ents:
            if ent.label_ in ["EVENT", "DISASTER"]:  # Common labels for incidents
                return ent.text.lower()

        # Check for keywords in the text if no entity is detected
        for keyword in incident_keywords:
            if keyword in text.lower():
                return keyword

        # Default fallback
        return "unknown"

    def process_data(self, input_file, output_file):
        """
        Process raw data and store standardized JSON in processed directory.
        """
        input_path = os.path.join(self.raw_data_path, input_file)
        output_path = os.path.join(self.processed_data_path, output_file)

        try:
            with open(input_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            processed_data = []
            for entry in raw_data:
                location_info = self.extract_location(entry.get("text", ""))
                incident_type = self.identify_incident_type(entry.get("text", ""))
                # Extract the original tweet's HTML address
                original_tweet = re.search(r"https://t\.co/\S+", entry.get("text", ""))
                original_tweet = original_tweet.group(0) if original_tweet else ""
                # Remove HTML addresses from description
                description = re.sub(r"https://t\.co/\S+", "", entry.get("text", "")).strip()

                standardized_entry = {
                    "incident_type": incident_type,
                    "location": location_info,
                    "date": entry.get("date", ""),  # Include the date from unprocessed data
                    "description": description,  # Description without HTML addresses
                    "original_tweet": original_tweet,  # Store only the HTML link
                }
                processed_data.append(standardized_entry)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=4)

            print(f"Processed data saved to {output_path}")

        except Exception as e:
            print(f"Error processing data: {e}")