job "autoscaler" {
  datacenters = ["dc1"]
  type = "service"

  group "worker-group" {
    count = 1

    network {
      port "api" {}
    }

    task "autoscaler-worker" {
      driver = "raw_exec"

      config {
        command = "C:\\Users\\Owner\\GolandProjects\\autoscaler\\worker.exe"
      }

      env {
        AUTOSCALER_CONFIG = "local/config.yaml"
      }

      template {
        data = <<EOF
api:
  address: "0.0.0.0:{{ env `NOMAD_PORT_api` }}"
rabbitmq:
  url: "amqp://guest:guest@127.0.0.1:5672/"
  queue_name: "events"
workers:
  initial_workers: 1
  min_workers: 1
  max_workers: 2
  initial_batch_size: 10
  min_batch_size: 10
  max_batch_size: 20
  batch_step: 10
processing:
  redis_cache_ttl: 15m
  redis_key_prefix: "autoscaler:event:"
mongodb:
  uri: "mongodb://127.0.0.1:27017"
  database: "autoscaler"
  collection: "events"
  connect_timeout: 5s
  health_check_enabled: true
  health_check_interval: 1s
  policy: "critical"
redis:
  addr: "127.0.0.1:6379"
  db: 0
  connect_timeout: 500ms
  health_check_enabled: true
  health_check_interval: 1s
  health_check_timeout: 500ms
  policy: "protective"
downstream:
  enabled: true
  observe_only: false
  degraded_latency: 250ms
  unhealthy_latency: 1s
  degraded_error_rate: 0.05
  unhealthy_error_rate: 0.20
  minimum_samples_for_state: 3
  degraded_consecutive_windows: 2
  unhealthy_consecutive_windows: 2
  healthy_consecutive_windows: 3
  decision_cooldown: 30s
scaling:
  tick_interval: 2s
  throughput_window_size: 5
  throughput_interval: 1s
  scale_up_lag_threshold: 70
  backpressure_lag_threshold: 100
  scale_down_lag_threshold: 20
  cpu_scale_up_threshold: 75
  cpu_backpressure_threshold: 85
  queue_growth_window: 5
  queue_growth_increase_count: 3
nomad:
  enabled: true
  address: "http://127.0.0.1:4646"
  job_name: "autoscaler"
  group_name: "worker-group"
  max_scale: 10
EOF
        destination = "local/config.yaml"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }
  }

  group "controller-group" {
    count = 1

    task "autoscaler-controller" {
      driver = "raw_exec"

      config {
        command = "C:\\Users\\Owner\\GolandProjects\\autoscaler\\controller.exe"
      }

      env {
        AUTOSCALER_CONFIG = "local/config.yaml"
      }

      template {
        data = <<EOF
api:
  address: "0.0.0.0:8081"
rabbitmq:
  url: "amqp://guest:guest@127.0.0.1:5672/"
  queue_name: "events"
workers:
  initial_workers: 1
  min_workers: 1
  max_workers: 2
  initial_batch_size: 10
  min_batch_size: 10
  max_batch_size: 20
  batch_step: 10
processing:
  redis_cache_ttl: 15m
  redis_key_prefix: "autoscaler:event:"
mongodb:
  uri: "mongodb://127.0.0.1:27017"
  database: "autoscaler"
  collection: "events"
  connect_timeout: 5s
  health_check_enabled: true
  health_check_interval: 1s
  policy: "critical"
redis:
  addr: "127.0.0.1:6379"
  db: 0
  connect_timeout: 500ms
  health_check_enabled: true
  health_check_interval: 1s
  health_check_timeout: 500ms
  policy: "protective"
downstream:
  enabled: true
  observe_only: false
  degraded_latency: 250ms
  unhealthy_latency: 1s
  degraded_error_rate: 0.05
  unhealthy_error_rate: 0.20
  minimum_samples_for_state: 3
  degraded_consecutive_windows: 2
  unhealthy_consecutive_windows: 2
  healthy_consecutive_windows: 3
  decision_cooldown: 30s
scaling:
  tick_interval: 2s
  throughput_window_size: 5
  throughput_interval: 1s
  scale_up_lag_threshold: 70
  backpressure_lag_threshold: 100
  scale_down_lag_threshold: 20
  cpu_scale_up_threshold: 75
  cpu_backpressure_threshold: 85
  queue_growth_window: 5
  queue_growth_increase_count: 3
nomad:
  enabled: true
  address: "http://127.0.0.1:4646"
  job_name: "autoscaler"
  group_name: "worker-group"
  max_scale: 10
EOF
        destination = "local/config.yaml"
      }

      resources {
        cpu    = 100
        memory = 128
      }
    }
  }
}
