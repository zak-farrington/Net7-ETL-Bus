# Net7-ETL-Bus
## .NET 7 Service Bus event-driven ETL process using TPL Dataflow

### Overview
An on-demand ETL process activated by an Azure Service Bus Queue trigger. After retrieving the initial dataset, the application adds information from third-party providers before storing the complete data into a PostgreSQL database.

This application demonstrates:
- An **Azure Service Bus** event-driven ETL process running as a **BackgroundService**
- Integration with third-party APIs
- **TPL Dataflow** data processing
- **Entity Framework (EF Core)** code-first data access

### To-Do
- **FTP** file retrieval of CSV
- Handle rate limiting
- Unit and integration testing with **xUnit**
- Organizing

### Dependencies
To run this project, you will need:

#### Services
- An Azure Service Bus Queue
- PostgresSQL Instance

#### API Keys
- Google API Key
