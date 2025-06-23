# Real-Time-Product-Repurchase-Scoring-System-using-Instacart-Dataset
Real-Time Product Repurchase Scoring System using Instacart Dataset

🎯 Goal of the Project
To simulate real-time product order events, stream them using Pub/Sub, process them in real-time with predictions using Apache Beam on Dataflow, and store the results in BigQuery for further analysis (e.g., via Looker Studio).

This mimics real-world use cases like:

Real-time product recommendation

Predictive supply chain monitoring

E-commerce churn or reordering prediction pipelines

🔧 Tools & Technologies Used

|Component|Purpose|
|Pub/Sub|Ingest real-time order events (like Kafka)|
|Apache Beam|Code framework for stream processing|
|Dataflow|Fully-managed runner for Apache Beam|
|BigQuery|Stores prediction results; supports analysis|
|Python|Language used for both simulation and pipeline|
Looker Studio (optional)	Visualize predictions in real time
