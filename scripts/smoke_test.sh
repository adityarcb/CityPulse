#!/bin/bash
# CityPulse Smoke Test Script
# Verifies the complete end-to-end pipeline
set -e

echo "=== CityPulse Smoke Test ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PASS=0
FAIL=0

pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS=$((PASS + 1))
}

fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAIL=$((FAIL + 1))
}

# 1. Check services are running
echo "1. Checking service health..."
for service in kafka postgres ingestion spark; do
    status=$(docker compose ps --format "{{.Name}} {{.Status}}" | grep "$service" | grep -o "Up.*" | head -1)
    if [ -n "$status" ]; then
        pass "$service is running"
    else
        fail "$service is not running"
    fi
done

# 2. Check Kafka topic exists
echo ""
echo "2. Checking Kafka topic..."
topic_exists=$(docker exec citypulse-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep "pollution-readings" || true)
if [ -n "$topic_exists" ]; then
    pass "pollution-readings topic exists"
else
    fail "pollution-readings topic missing"
fi

# 3. Check PostgreSQL connectivity
echo ""
echo "3. Checking PostgreSQL..."
db_ready=$(docker exec citypulse-postgres-1 pg_isready -U citypulse -d citypulse 2>/dev/null | grep "accepting connections" || true)
if [ -n "$db_ready" ]; then
    pass "PostgreSQL is accepting connections"
else
    fail "PostgreSQL not accepting connections"
fi

# 4. Check tables exist
echo ""
echo "4. Checking database tables..."
for table in pollution_readings pollution_alerts pollution_aggregates; do
    count=$(docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -t -c "SELECT to_regclass('$table');" 2>/dev/null | tr -d ' ')
    if [ "$count" = "$table" ]; then
        pass "Table $table exists"
    else
        fail "Table $table missing"
    fi
done

# 5. Send test event and verify ingestion (with retries for consumer warm-up)
echo ""
echo "5. Testing end-to-end event processing..."
test_city="SmokeTest_$(date +%s)"
test_event=$(printf '{"city":"%s","pm25":120.0,"temperature":28.0}' "$test_city")
echo "$test_event" | docker exec -i citypulse-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic pollution-readings 2>/dev/null

reading_count=0
for i in $(seq 1 6); do
    sleep 5
    reading_count=$(docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -t -c "SELECT COUNT(*) FROM pollution_readings WHERE city = '$test_city';" 2>/dev/null | tr -d ' ')
    if [ "$reading_count" = "1" ]; then
        break
    fi
done
if [ "$reading_count" = "1" ]; then
    pass "Test event ingested correctly"
else
    fail "Test event not ingested (count: $reading_count)"
fi

# 6. Check alert was generated (with retries)
echo ""
echo "6. Testing alert generation..."
alert_count=0
for i in $(seq 1 6); do
    sleep 5
    alert_count=$(docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -t -c "SELECT COUNT(*) FROM pollution_alerts WHERE city = '$test_city';" 2>/dev/null | tr -d ' ')
    if [ "$alert_count" = "1" ]; then
        break
    fi
done
if [ "$alert_count" = "1" ]; then
    pass "Alert generated for test event"
else
    fail "Alert not generated (count: $alert_count)"
fi

# 7. Test idempotency - replay same event
echo ""
echo "7. Testing idempotency..."
echo "$test_event" | docker exec -i citypulse-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic pollution-readings 2>/dev/null

reading_count_after=0
for i in $(seq 1 6); do
    sleep 5
    reading_count_after=$(docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -t -c "SELECT COUNT(*) FROM pollution_readings WHERE city = '$test_city';" 2>/dev/null | tr -d ' ')
    if [ "$reading_count_after" = "1" ]; then
        break
    fi
done
if [ "$reading_count_after" = "1" ]; then
    pass "Idempotency verified - no duplicate reading"
else
    fail "Duplicate reading created (count: $reading_count_after)"
fi

# 8. Check Spark aggregates exist
echo ""
echo "8. Checking Spark aggregates..."
agg_count=$(docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -t -c "SELECT COUNT(*) FROM pollution_aggregates;" 2>/dev/null | tr -d ' ')
if [ -n "$agg_count" ] && [ "$agg_count" -gt 0 ]; then
    pass "Spark aggregates exist ($agg_count windows)"
else
    fail "No Spark aggregates found"
fi

# Cleanup test data
docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -c "DELETE FROM pollution_alerts WHERE city = '$test_city';" 2>/dev/null
docker exec citypulse-postgres-1 psql -U citypulse -d citypulse -c "DELETE FROM pollution_readings WHERE city = '$test_city';" 2>/dev/null

echo ""
echo "=== Smoke Test Results ==="
echo -e "${GREEN}Passed: $PASS${NC}"
echo -e "${RED}Failed: $FAIL${NC}"

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}All smoke tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some smoke tests failed!${NC}"
    exit 1
fi
