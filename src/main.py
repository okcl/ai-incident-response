from src.processors.data_processor import DataProcessor

if __name__ == "__main__":
    processor = DataProcessor()
    processor.process_data("top_disaster_posts.json", "processed_posts.json")
