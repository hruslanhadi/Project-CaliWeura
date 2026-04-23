# =====================================================
# ARCHITECTURE DECISION RECORD (ADR)
# =====================================================
# Documentation of configuration choices

## ADR-001: Environment-Specific Configurations

### Problem
- Credentials were hardcoded in docker-compose.yml
- Single configuration couldn't handle dev vs prod requirements
- Resource allocation not optimized for different environments

### Solution
- Separate environment files (.env.development, .env.production, .env.example)
- Separate docker-compose files (docker-compose.dev.yml, docker-compose.prod.yml)
- Environment variable substitution for all sensitive data

### Trade-offs
- More files to maintain
- Must explicitly select environment when running docker-compose
+ Better security
+ Flexible resource allocation
+ Clear separation of concerns

---

## ADR-002: Network Segmentation

### Problem
- All services on default bridge network
- No isolation between layers
- Difficult to implement security policies

### Solution
- Three separate networks:
  - `dp_backend`: Airflow, PostgreSQL, Redis
  - `dp_frontend`: User-facing services (Webserver)
  - `dp_data`: Data processing (Spark, Kafka, MinIO)

### Benefits
+ Security isolation
+ Better troubleshooting
+ Prevents accidental cross-network access
+ Aligns with architecture layers

---

## ADR-003: PostgreSQL User Separation

### Problem
- Single user for Airflow and warehouse
- Violates principle of least privilege
- Difficult to audit and control access

### Solution
- Three separate users:
  - `airflow_meta`: Airflow metadata (full access to airflow db)
  - `warehouse_user`: Warehouse operations (full access to data_warehouse)
  - `analytics_user`: Analytics queries (read-only to data_warehouse)

### Benefits
+ Fine-grained access control
+ Better audit trail
+ Limits damage from compromised accounts
+ Follows security best practices

---

## ADR-004: CeleryExecutor vs LocalExecutor

### Problem
- LocalExecutor limited to single machine
- Need scalability for production
- Resource constraints on development laptop

### Solution
- **Development**: LocalExecutor + Redis (easy to debug, minimal resources)
- **Production**: CeleryExecutor + Redis + Worker nodes (scalable, distributed)

### Configuration
- Development: AIRFLOW__CORE__EXECUTOR=LocalExecutor
- Production: AIRFLOW__CORE__EXECUTOR=CeleryExecutor

### Trade-offs
- Development: Simpler setup, easier debugging, limited parallelism
- Production: Complex setup, distributed debugging, unlimited scalability

---

## ADR-005: Health Checks for All Services

### Problem
- Docker Compose couldn't determine service readiness
- Race conditions during startup
- No automatic recovery from failed services

### Solution
- Add health checks to all critical services
- Use proper startup dependencies (service_healthy, service_completed_successfully)
- Enable automatic restarts

### Benefits
+ Reliable startup sequence
+ Automatic recovery
+ Better monitoring
+ Services only considered "ready" when actually functional

---

## ADR-006: Structured JSON Logging for Production

### Problem
- Text logs difficult to parse and search
- No structured metadata
- Incompatible with centralized logging (ELK, Loki)

### Solution
- Development: Simple text logging to files/console
- Production: Structured JSON logging with full context

### Benefits
+ Easy parsing and filtering
+ Compatible with log aggregation
+ Better for debugging
+ Can include custom context fields

---

## ADR-007: Official Apache Spark Image

### Problem
- Bitnamilegacy/spark is unmaintained
- Security vulnerabilities
- Missing recent features

### Solution
- Use official apache/spark:3.4.1 image
- Maintained and regular security updates
- Better documentation

### Migration Path
- Rebuild with new base image
- Test all Spark jobs
- Monitor for compatibility issues

---

## ADR-008: Resource Allocation Strategy

### Development (16GB Laptop)
```
PostgreSQL: 512MB / 0.5 CPU
MinIO: 512MB / 0.5 CPU
Spark: 1GB / 0.8 CPU
Airflow Web: 1GB / 0.8 CPU
Airflow Scheduler: 1GB / 0.8 CPU
Redis: 256MB / 0.3 CPU
Total: ~5GB / 4.7 CPU
```

### Production
```
PostgreSQL: 4GB / 2 CPU
MinIO: 2GB / 1.5 CPU
Spark: 8GB / 4 CPU
Airflow Web: 2GB / 2 CPU
Airflow Scheduler: 3GB / 2 CPU
Airflow Worker: 2GB / 2 CPU
Redis: 1GB / 1 CPU
Prometheus: 1GB / 1 CPU
Grafana: 512MB / 0.5 CPU
Total: ~24GB / 16.5 CPU
```

---

## ADR-009: Monitoring Stack

### Development
- Minimal logging
- Optional metrics collection
- Focus on quick feedback

### Production
- Full observability stack:
  - Prometheus for metrics
  - Grafana for visualization
  - Loki for log aggregation
  - Alerts for critical conditions

---

## ADR-010: Backup and Recovery Strategy

### Backup Targets
1. PostgreSQL: Daily dumps
2. MinIO: Weekly snapshots
3. Configuration: Version controlled (Git)
4. Docker volumes: Monthly full backups

### Recovery Time Objective (RTO)
- Development: Not critical
- Production: 4 hours

### Recovery Point Objective (RPO)
- Development: Not critical
- Production: 24 hours

---

## Decision Matrix

| Aspect | Development | Production |
|--------|-------------|-----------|
| **Executor** | LocalExecutor | CeleryExecutor |
| **RAM** | 512MB-1GB per service | 1GB-4GB per service |
| **Logging** | Text files | JSON structured |
| **Monitoring** | Optional | Required |
| **Backups** | Manual | Automated daily |
| **High Availability** | Single instance | Multi-instance ready |
| **Network** | Single bridge | Segmented networks |
| **Security** | Development grade | Production hardened |
| **Users** | Shared credentials | Separate users |
| **TTL** | Temporary | Permanent |

---

## Implementation Checklist

- [x] Environment-specific configuration files
- [x] Separate docker-compose files (dev/prod)
- [x] Health checks on all services
- [x] Network segmentation
- [x] PostgreSQL user separation
- [x] Database initialization scripts
- [x] Dockerfile improvements
- [x] Logging configurations
- [x] Monitoring setup (Prometheus/Grafana)
- [x] Runbooks and documentation
- [ ] Backup automation
- [ ] Alert rules
- [ ] Security scanning
- [ ] Disaster recovery testing
