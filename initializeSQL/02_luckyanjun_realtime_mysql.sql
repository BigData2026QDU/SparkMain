CREATE DATABASE IF NOT EXISTS bigdata_ana
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE bigdata_ana;

CREATE TABLE IF NOT EXISTS lb_realtime_batches (
  checkpoint_key VARCHAR(255) NOT NULL,
  batch_id BIGINT NOT NULL,
  processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (checkpoint_key, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lb_realtime_window_users (
  window_start DATETIME NOT NULL,
  user_id BIGINT NOT NULL,
  is_pv_user TINYINT NOT NULL DEFAULT 0,
  is_buy_user TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (window_start, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lb_realtime_window_metrics (
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  pv BIGINT NOT NULL DEFAULT 0,
  approx_uv BIGINT NOT NULL DEFAULT 0,
  fav_cnt BIGINT NOT NULL DEFAULT 0,
  cart_cnt BIGINT NOT NULL DEFAULT 0,
  buy_cnt BIGINT NOT NULL DEFAULT 0,
  buy_uv BIGINT NOT NULL DEFAULT 0,
  pv_to_buy_rate DECIMAL(12,4) NOT NULL DEFAULT 0,
  alert_type VARCHAR(32) NOT NULL DEFAULT 'normal',
  alert_message VARCHAR(255) NOT NULL DEFAULT '',
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (window_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lb_realtime_category_stats (
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  category_id BIGINT NOT NULL,
  event_cnt BIGINT NOT NULL DEFAULT 0,
  buy_cnt BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (window_start, category_id),
  KEY idx_category_id_rank (window_start, event_cnt, buy_cnt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lb_realtime_item_stats (
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  item_id BIGINT NOT NULL,
  event_cnt BIGINT NOT NULL DEFAULT 0,
  buy_cnt BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (window_start, item_id),
  KEY idx_item_id_rank (window_start, event_cnt, buy_cnt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lb_realtime_category_top10 (
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  rank_no INT NOT NULL,
  category_id BIGINT NOT NULL,
  event_cnt BIGINT NOT NULL,
  buy_cnt BIGINT NOT NULL,
  PRIMARY KEY (window_start, category_id),
  UNIQUE KEY uk_window_rank (window_start, rank_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lb_realtime_item_top10 (
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  rank_no INT NOT NULL,
  item_id BIGINT NOT NULL,
  event_cnt BIGINT NOT NULL,
  buy_cnt BIGINT NOT NULL,
  PRIMARY KEY (window_start, item_id),
  UNIQUE KEY uk_window_rank (window_start, rank_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
