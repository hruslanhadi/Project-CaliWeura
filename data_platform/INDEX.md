# 📚 Documentation Index

Welcome to the Data Platform! This index will help you navigate all documentation and get started quickly.

## 🚀 Getting Started (Read These First)

1. **[QUICKSTART.md](QUICKSTART.md)** ⭐ START HERE
   - 60-second setup guide
   - First run checklist
   - Common commands
   - Troubleshooting quick fixes
   - **Reading Time**: 5 minutes

2. **[README.md](README.md)** - Complete Reference
   - Full architecture overview
   - System requirements
   - Project structure
   - Quick start detailed
   - Pipeline overview
   - Data model documentation
   - Example SQL queries
   - Monitoring & observability
   - Performance tuning
   - Production deployment
   - **Reading Time**: 30-45 minutes

## 🏗️ Architecture & Design

3. **[ARCHITECTURE.md](ARCHITECTURE.md)** - Deep Dive
   - System components details
   - Data flow & transformations
   - Configuration management
   - Error handling & resilience
   - Performance characteristics
   - Security & authentication
   - Monitoring & alerting
   - Disaster recovery
   - Integration patterns
   - **Reading Time**: 45-60 minutes

## 📊 Visual Guides

4. **[PROJECT_MAP.md](PROJECT_MAP.md)** - Visual Reference
   - Complete folder structure
   - Services architecture diagram
   - Data pipeline flow
   - Star schema visualization
   - File statistics
   - Technology versions
   - Quick reference commands
   - **Reading Time**: 15 minutes

## 📁 Project Structure

```
data_platform/
├── 📖 QUICKSTART.md              👈 START HERE
├── 📖 README.md                  Complete docs
├── 📖 ARCHITECTURE.md            Technical deep dive
├── 📖 PROJECT_MAP.md             Visual guide
├── 📖 INDEX.md                   This file
│
├── docker-compose.yml            Main orchestration
├── Dockerfile.airflow            Container image
├── requirements.txt              Python dependencies
├── .env                          Environment config
│
├── dags/
│   └── data_platform_medallion_pipeline.py    Main DAG
│
├── spark_jobs/
│   ├── 01_ingest_*.py            Bronze layer
│   ├── 02_transform_*.py         Silver layer
│   └── 03_aggregate_to_warehouse.py Gold layer
│
├── scripts/
│   ├── startup.sh                Initialize platform
│   ├── cleanup.sh                Clean up
│   ├── health_check.sh           Monitor health
│   └── generate_data.py          Generate test data
│
└── configs/
    ├── config.py                 App configuration
    └── platform_config.yaml      Platform config
```

## 🎯 Common Tasks

### First Time Setup
1. Read [QUICKSTART.md](QUICKSTART.md)
2. Run `./scripts/startup.sh`
3. Access http://localhost:8080
4. Enable and trigger DAG

### Understanding the Architecture
1. Start with [README.md](README.md) - Overview section
2. Read [ARCHITECTURE.md](ARCHITECTURE.md) - System Components
3. Review [PROJECT_MAP.md](PROJECT_MAP.md) - Visual diagrams

### Running the Pipeline
1. Review [README.md](README.md) - Pipeline Overview section
2. Go to http://localhost:8080
3. Enable `data_platform_medallion_pipeline` DAG
4. Trigger manual run
5. Monitor in Graph view

### Querying Results
1. Read [README.md](README.md) - Example Queries section
2. Connect: `psql -h localhost -U warehouse_user -d data_warehouse`
3. Run provided SQL examples

### Troubleshooting Issues
1. Check [README.md](README.md) - Troubleshooting section
2. Run `./scripts/health_check.sh`
3. View logs: `docker-compose logs <service>`
4. Read error messages and match against docs

### Extending the Platform
1. Read [README.md](README.md) - Contributing section
2. Review [ARCHITECTURE.md](ARCHITECTURE.md) - Integration Patterns
3. Follow examples in existing spark_jobs/
4. Test with tests/ suite

### Performance Tuning
1. See [README.md](README.md) - Performance Tuning section
2. Check [ARCHITECTURE.md](ARCHITECTURE.md) - Performance Characteristics
3. Monitor with `docker stats`

## 📞 Quick Reference

### Access Points

| Service | URL | Login |
|---------|-----|-------|
| **Airflow** | http://localhost:8080 | admin/admin123 |
| **MinIO** | http://localhost:9001 | minioadmin/minioadmin_secure_pass |
| **Spark Master** | http://localhost:8081 | - |
| **PostgreSQL** | localhost:5432 | warehouse_user/warehouse_pass |
| **Kafka** | localhost:9092 | - |

### Essential Commands

```bash
# Startup
docker-compose up -d

# Monitoring
docker-compose ps
./scripts/health_check.sh
docker-compose logs -f airflow_scheduler

# Generate test data
python scripts/generate_data.py

# Connect to database
psql -h localhost -U warehouse_user -d data_warehouse -W

# Cleanup
./scripts/cleanup.sh
```

### Key Files

- **Docker Compose**: `docker-compose.yml` (service definitions)
- **Main DAG**: `dags/data_platform_medallion_pipeline.py`
- **Spark Jobs**: `spark_jobs/*.py` (7 jobs)
- **Database Schema**: `scripts/init_db.sql`
- **Python Config**: `configs/config.py`
- **Environment**: `.env` (all secrets & config)

## 📚 Reading Recommendations by Role

### For Data Engineers
1. QUICKSTART.md
2. README.md (focus on Pipeline, Data Model, Spark Jobs sections)
3. ARCHITECTURE.md (System Components, Data Flow)
4. Spark job files (spark_jobs/*.py)

### For DevOps Engineers
1. QUICKSTART.md
2. README.md (Docker Setup, Monitoring, Performance Tuning)
3. ARCHITECTURE.md (complete)
4. docker-compose.yml and Dockerfile.airflow

### For Data Analysts
1. QUICKSTART.md
2. README.md (Example Queries section)
3. Database schema (scripts/init_db.sql)
4. PostgreSQL tips in README

### For System Architects
1. README.md (complete)
2. ARCHITECTURE.md (complete)
3. PROJECT_MAP.md (complete)
4. docker-compose.yml

## ✅ Verification Checklist

After setting up, verify:
- [ ] All services running: `docker-compose ps` (all showing "running")
- [ ] Health check passes: `./scripts/health_check.sh`
- [ ] Airflow accessible: http://localhost:8080
- [ ] Test data generated: `ls -lh data/`
- [ ] PostgreSQL accessible: `psql -h localhost -U warehouse_user -d data_warehouse -c "SELECT 1;"`
- [ ] MinIO accessible: http://localhost:9001
- [ ] DAG shows in Airflow: Search for `data_platform_medallion_pipeline`

## 🆘 Getting Help

1. **Service won't start?**
   - Read: README.md → Troubleshooting
   - Run: `./scripts/health_check.sh`
   - Check: `docker system df` (disk space)

2. **DAG not visible?**
   - Read: README.md → Troubleshooting → Airflow DAG Not Showing
   - Command: `docker-compose logs airflow_scheduler`

3. **Can't connect to database?**
   - Read: README.md → Troubleshooting → PostgreSQL Connection Issues
   - Command: `docker-compose exec postgres pg_isready -U warehouse_user`

4. **Need performance tuning?**
   - Read: README.md → Performance Tuning
   - Check: ARCHITECTURE.md → Performance Characteristics
   - Monitor: `docker stats`

## 📖 Documentation Standards

All documentation in this project follows:
- **Clear structure**: Headers, bullet points, code blocks
- **Practical examples**: Real commands, actual queries
- **Visual aids**: Diagrams, tables, ASCII art
- **Complete context**: Explains not just "how" but "why"
- **Cross-references**: Links between related topics
- **Actionable**: Every section has concrete next steps

## 🎓 Learning Path

**Week 1: Basics**
- Day 1: QUICKSTART.md + get it running
- Day 2: README.md (Overview, Architecture, Components)
- Day 3: Explore UIs (Airflow, MinIO, PostgreSQL)
- Day 4: Run the pipeline, watch execution
- Day 5: Query results, run example queries
- Day 6-7: Explore code, understand data flow

**Week 2: Deep Dive**
- Day 1-2: ARCHITECTURE.md + study system design
- Day 3-4: Review Spark jobs, understand transformations
- Day 5: Study DAG structure, Airflow concepts
- Day 6: Database schema, star schema design
- Day 7: Performance tuning, monitoring

**Week 3: Extending**
- Day 1-2: Add new data source (new ingestion job)
- Day 3-4: Create new transformation
- Day 5-6: Add new dimension/fact table
- Day 7: Deploy and monitor changes

**Week 4: Production**
- Day 1-2: Load testing, scale validation
- Day 3-4: Production deployment planning
- Day 5-6: Security audit, optimization
- Day 7: Runbook development, monitoring setup

## 📋 Document Versions

| Document | Version | Updated | Size |
|----------|---------|---------|------|
| QUICKSTART.md | 1.0 | 2024-04-22 | 2KB |
| README.md | 1.0 | 2024-04-22 | 50KB |
| ARCHITECTURE.md | 1.0 | 2024-04-22 | 30KB |
| PROJECT_MAP.md | 1.0 | 2024-04-22 | 15KB |
| INDEX.md | 1.0 | 2024-04-22 | 10KB |

## 🎯 Success Metrics

You'll know you're ready when:
- ✅ All services running without errors
- ✅ DAG executing successfully
- ✅ Data visible in PostgreSQL
- ✅ Can run provided SQL queries
- ✅ Understand the medallion architecture
- ✅ Can modify Spark jobs
- ✅ Can add new data sources
- ✅ Can write queries for analysis

---

**Now go forth and build amazing data pipelines! 🚀**

Start with [QUICKSTART.md](QUICKSTART.md)
