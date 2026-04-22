# Quick Start Guide

## 60-Second Setup

```bash
# 1. Navigate to project
cd data_platform

# 2. Make scripts executable
chmod +x scripts/*.sh

# 3. Start services
docker-compose up -d

# 4. Wait for readiness (2-3 minutes)
sleep 30

# 5. Initialize data
docker-compose exec airflow_webserver python scripts/generate_data.py

# 6. Access Airflow
# Open: http://localhost:8080
# Login: admin / admin123
```

## First Run Checklist

- [ ] Docker & Docker Compose installed
- [ ] All services started: `docker-compose ps` (all running)
- [ ] Health check passed: `./scripts/health_check.sh`
- [ ] Test data generated: `ls -lh data/`
- [ ] Airflow UI accessible: http://localhost:8080

## Run Your First Pipeline

1. **Go to Airflow** → http://localhost:8080
2. **Find DAG** → Search for `data_platform_medallion_pipeline`
3. **Enable DAG** → Toggle the blue switch on the left
4. **Trigger Run** → Click blue play button, then "Trigger"
5. **Monitor** → Watch progress in "Graph" tab
6. **Expected Duration** → 3-5 minutes

## Access All UIs

| Service | URL | Login |
|---------|-----|-------|
| **Airflow** | http://localhost:8080 | admin/admin123 |
| **MinIO** | http://localhost:9001 | minioadmin/minioadmin_secure_pass |
| **Spark** | http://localhost:8081 | - |

## Query Results in PostgreSQL

```bash
# Connect to database
psql -h localhost -U warehouse_user -d data_warehouse -W
# Password: warehouse_pass

# Check loaded data
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_products;

# Sample query
SELECT 
    dc.first_name, 
    dc.last_name, 
    SUM(fs.total_amount) as total_spent
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_id, dc.first_name, dc.last_name
ORDER BY total_spent DESC
LIMIT 10;
```

## Common Commands

```bash
# View all services
docker-compose ps

# View logs
docker-compose logs -f airflow_scheduler

# Stop services
docker-compose stop

# Clean up everything
./scripts/cleanup.sh

# Health check
./scripts/health_check.sh

# Restart specific service
docker-compose restart spark_master
```

## Troubleshooting

**Services won't start?**
```bash
docker system prune -a
docker-compose pull
docker-compose up -d
```

**Out of memory?**
- Close other applications
- Check available RAM: `free -h`
- Reduce executor memory in `docker-compose.yml`

**DAG not showing?**
```bash
docker-compose logs airflow_scheduler | grep ERROR
```

**Can't connect to PostgreSQL?**
```bash
docker-compose exec postgres pg_isready -U warehouse_user
```

## Next Steps

1. **Review** the main [README.md](README.md)
2. **Explore** [ARCHITECTURE.md](ARCHITECTURE.md) for deep dive
3. **Modify** DAGs in `dags/` folder
4. **Add** new Spark jobs in `spark_jobs/`
5. **Deploy** to production (see README deployment section)

---

**Ready to go!** 🚀
