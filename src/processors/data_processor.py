import os
import json
import spacy
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
                result = geocode(ent.text, max_locations=1, as_featureset=True, gis=self.gis)
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
        Identify incident type based on keywords in text.
        """
        keywords = {
            "flood": "flooding",
            "heavy rain": "flooding",
            "inundation": "flooding",
            "waterlogging": "flooding",
            "hurricane": "hurricane",
            "typhoon": "hurricane",
            "cyclone": "hurricane",
            "tornado": "tornado",
            "twister": "tornado",
            "storm": "storm",
            "thunderstorm": "storm",
            "hailstorm": "storm",
            "blizzard": "storm",
            "snowstorm": "storm",
            "drought": "drought",
            "heatwave": "drought",
            "wildfire": "wildfire",
            "bushfire": "wildfire",
            "forest fire": "wildfire",
            "earthquake": "earthquake",
            "seismic": "earthquake",
            "quake": "earthquake",
            "tsunami": "tsunami",
            "tidal wave": "tsunami",
            "volcano": "volcanic eruption",
            "eruption": "volcanic eruption",
            "lava": "volcanic eruption",
            "ashfall": "volcanic eruption",
            "landslide": "landslide",
            "mudslide": "landslide",
            "avalanche": "avalanche",
            "collapse": "building collapse",
            "building collapse": "building collapse",
            "sinkhole": "sinkhole",
            "pandemic": "pandemic",
            "epidemic": "epidemic",
            "disease outbreak": "epidemic",
            "fire": "fire",
            "explosion": "explosion",
            "blast": "explosion",
            "chemical spill": "hazardous material",
            "gas leak": "hazardous material",
            "radiation leak": "hazardous material",
            "toxic release": "hazardous material",
            "industrial accident": "industrial accident",
            "train crash": "transportation accident",
            "plane crash": "transportation accident",
            "shipwreck": "transportation accident",
            "road accident": "transportation accident",
            "car crash": "transportation accident",
            "vehicle collision": "transportation accident",
            "traffic incident": "transportation accident",
            "terrorist attack": "terrorism",
            "bombing": "terrorism",
            "shooting": "terrorism",
            "hostage": "terrorism",
            "cyberattack": "cyberattack",
            "data breach": "cyberattack",
            "phishing": "cyberattack",
            "power outage": "infrastructure failure",
            "blackout": "infrastructure failure",
            "water shortage": "infrastructure failure",
            "bridge collapse": "infrastructure failure",
            "dam breach": "infrastructure failure",
            "riot": "civil unrest",
            "protest": "civil unrest",
            "demonstration": "civil unrest",
            "stampede": "civil unrest"
        }
        for keyword, incident_type in keywords.items():
            if keyword in text.lower():
                return incident_type
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
                standardized_entry = {
                    "incident_type": incident_type,
                    "location": location_info,
                    "date": entry.get("created_at", ""),
                    "description": entry.get("text", ""),
                    "original_text": entry.get("text", "")
                }
                processed_data.append(standardized_entry)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=4)

            print(f"Processed data saved to {output_path}")

        except Exception as e:
            print(f"Error processing data: {e}")
