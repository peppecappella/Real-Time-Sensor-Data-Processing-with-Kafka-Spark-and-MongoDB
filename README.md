# Real Time Sensor Data Processing with Kafka Spark and MongoDB

Author: Giuseppe Cappella

This project aims to capture, process, and visualize in real-time sensor data coming from bike-sharing stations. Using an architecture based on Kafka, Spark, and MongoDB, the system is designed to handle real-time data streams, enrich them with contextual information, and aggregate them for analysis and visualization through interactive dashboards.

## Data Source 

The data source for this project is the API provided by Capital Bikeshare, a leading bike-sharing service that operates in the Washington D.C. metropolitan area, including parts of Maryland and Virginia.

Capital Bikeshare was launched in 2010 and has since experienced rapid growth. Today, it boasts thousands of bicycles and hundreds of bike-sharing stations across the region. This expansive network serves as an invaluable resource for our project, offering real-time access to comprehensive data on bike-sharing usage, station activity, and bicycle availability.

For more information and to explore Capital Bikeshare further, follow this to their website: [Capital Bikeshare](https://capitalbikeshare.com/system-data).

## Data Flow and Processing

The data flow consists of 4 principal steps:

**Data Capture:** The system begins with the Kafka producer component, which makes periodic HTTP requests to obtain updated data from bike-sharing stations. The data is serialized into JSON and published on a Kafka topic.

**Data Processing and Enrichment:** The data is consumed by the Spark component, which processes it in streaming. This includes deserializing messages, enriching them with additional information about the stations, and aggregating the data. The results are then saved in MongoDB.

**Data Aggregation:** Further Python scripts, executed periodically, read the data from MongoDB to perform hourly aggregations and save the results in dedicated collections. These steps prepare the data for more complex analyses and for visualization.

**Visualization:** Dash dashboards display the aggregated data through bar, pie, and line charts. These dashboards provide real-time updates and detailed insights into the usage of bike-sharing stations.

## Architecture and Technologies

**Apache Kafka:** Used as a distributed messaging system to manage the flow of data in real-time. Kafka acts as the backbone for data ingestion, enabling scalable message production and consumption. Sensor data is captured by the Kafka producer and sent to specific topics, where it can be consumed by various system components.

**Apache Spark:** Employed for data stream processing. Spark Structured Streaming allows for the efficient processing of real-time data from Kafka, performing transformations, aggregations, and enrichments. Spark is known for its ability to process large volumes of data with low latency, making it ideal for real-time analysis.

**MongoDB:** A NoSQL database used to store and manage processed data. MongoDB is chosen for its flexibility in handling variable data schemas, horizontal scalability, and its indexing features, including Time-To-Live (TTL) indexes, which automate the removal of outdated data.

**Dash:** A Python framework for developing analytical web applications. Used to create interactive dashboards for data visualization. Dash integrates closely with Plotly for chart generation and supports real-time callbacks for dynamic content updating.

**Docker:** An open-source platform for developing, shipping, and running applications in containers. Containers allow a developer to package up an application with all of the parts it needs, such as libraries and other dependencies, and ship it all out as one package. In our system, services including Kafka, Spark, and MongoDB are hosted on Docker containers. This approach enhances the system's scalability and ensures a consistent environment across different stages, from development to production. To enable seamless interaction and connectivity, Docker is configured to open ports on localhost. This configuration ensures that the services are easily accessible for development and testing purposes, thereby simplifying the workflow and enhancing productivity.

## Requirements

Before starting with the installation and configuration steps, ensure that you have the following prerequisites installed on your system:

- Docker: Required for running the services in isolated containers.
- Java: Necessary for Kafka to run. Make sure you have Java 8 or newer installed.
- Python: Required for running the scripts.
- Jupyter: Recommended if you want to run the scripts as jupyter notebooks.

## Installation and Configuration Steps

Follow these steps to set up the environment and get the system up and running.

### Docker Network Creation:  

Start by creating a network in Docker. This will allow your containers to communicate with each other. Open your terminal and run the following command:

docker network create kafka-network

### Zookeeper Container: 

Launch a Zookeeper container, which Kafka will use for coordination and management. Execute the following command:

docker run -d --name zookeeperlst --network kafka-network -p 2181:2181 -e ALLOW_ANONYMOUS_LOGIN=yes bitnami/zookeeper:latest

### Kafka Container:

Next, start a Kafka container within the same network. This step is crucial for setting up the message broker system. Use the command:

docker run -d --name kafkalst --network kafka-network -p 9092:9092 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeperlst:2181 -e ALLOW_PLAINTEXT_LISTENER=yes bitnami/kafka:latest

### MongoDB Container:

Finally, deploy a MongoDB container, which will serve as your database for storing processed data. Run:

docker run -d --name mongodb --network kafka-network -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=mongoadmin -e MONGO_INITDB_ROOT_PASSWORD=secret mongo:latest

### Setting JAVA_HOME Environment Variable:

This step is necessary for Kafka to run properly. The process to set this variable varies depending on your operating system.

**Check if Java is already installed:** 

Open a terminal or command prompt and type java -version. If you get an output that shows the version of Java installed, it means that Java is already present on your system. If, on the other hand, you receive an error message or a message indicating that the command is not recognized, you will need to install Java.

**Java Installation:** 

If Java is not installed, you can download and install it from the official Oracle Java site (link) or use OpenJDK (link). Choose the version of Java that best suits your needs (for example, Java 8, 11, or later versions are commonly used with Spark).

**Setting the JAVA_HOME environment variable:**

### On Windows:

- a. Search for "Environment Variables" in the Start menu and select "Edit the system environment variables".
- b. In the "System Properties" window, click on "Environment Variables".
- c. Click on "New" under "System variables" and enter JAVA_HOME as the variable name and the path of the Java installation folder (for example, C:\Program Files\Java\jdk-11.0.1) as the value.
- d. Modify the Path variable by adding %JAVA_HOME%\bin to the existing value.

### On Linux/Mac:

- a. Open your shell's configuration file (for example, ~/.bashrc, ~/.zshrc, etc.) with a text editor.
- b. Add the following lines at the end of the file (replacing /path/to/your/jdk with the actual path of your JDK installation):

export JAVA_HOME=/path/to/your/jdk
export PATH=$JAVA_HOME/bin:$PATH

- c. Save the file and execute source ~/.bashrc (or the name of your shell's configuration file) to apply the changes.

**Check the configuration:**

After setting JAVA_HOME, reopen the terminal or command prompt and type java -version again to verify that Java is correctly installed and recognized by the system.

## RUNNING THE SCRIPTS:

After completing these steps, your environment will be configured, and you should be able to start working with Kafka, Spark, and MongoDB for real-time ETL processing of sensor data from bike-sharing stations.

Open a jupyter notebook on the main folder and execute in this order:

1) producer.ipynb
2) consumer.ipynb
3) pipeline_1.ipynb
4) pipeline_2.ipynb
5) dashboard.ipynb

## Project Results ##

A first result of the project is the creation and continuous update of three mongo collections:

1) The "sensor_data" collection contains raw information on the status of bike sharing stations updated with a latency of one second.
2) The "archived hourly departures" collection contains the estimated count of the number of hourly departures from the stations.
3) The "archived hourly departures per station" collection contains information from the archived hourly departures collection aggregated by station and date.

**Here is a preview of the mongo collections created (one can run the scripts and visualize the collection using native mongo or a MongoDB Compass application, username:admin, password:secret):**


<img width="1391" alt="Schermata 2024-03-04 alle 02 10 17" src="https://github.com/peppecappella/Sensor-Data-Real-Time-ETL-Processing-with-Kafka-Spark-and-MongoDB/assets/124899610/d3a8029a-afbb-4053-9f5b-99210edd1a06">

**Continuous Data Collection:**

A core feature of the system is its continuous data collection mechanism. The application remains active, gathering data from bike-sharing stations until explicitly stopped by the user. This persistent operation allows for the accumulation of a rich dataset over time, providing valuable insights into usage patterns and station activity.

**Use Cases:**

Such a comprehensive data series serves as a foundational resource for various analytical and predictive tasks. Future applications of this dataset could include:

- Regression Models Development: Leveraging the historical data to predict future bike-sharing usage patterns, helping in resource allocation and station management.

- Integration with Weather Data: Combining bike-sharing data with weather information to analyze the impact of weather conditions on bike usage, enabling more accurate predictions and planning.

- Further Aggregation for Advanced Analyses: The dataset can be enriched with additional data sources, such as traffic patterns or public events, to explore broader mobility trends and behaviors.

**Real-Time Data Visualization Dashboard**

Another result of the project is the development of an interactive dashboard. This dashboard provides real-time updates and detailed insights into the bike-sharing usage, showcasing:

- A bar chart showcasing the top 20 stations by changes in the number of available bicycles, giving a clear view of fluctuations in bicycle availability.

- A pie chart representing the top 20 stations by total number of departures, offering a snapshot of service usage intensity.

- A line chart depicting hourly departures from the top 20 stations over the last available day, allowing observation of usage trends throughout the day.

**Here is a preview of the final dashboard:**


![dashhh](https://github.com/peppecappella/Sensor-Data-Real-Time-ETL-Processing-with-Kafka-Spark-and-MongoDB/assets/124899610/1fcd727c-dd9e-4a1f-9433-1e3ec70880ba)



