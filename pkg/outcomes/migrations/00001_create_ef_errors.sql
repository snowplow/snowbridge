-- +goose Up
create table ef_errors (
  id           bigserial primary key,
  received_at  timestamptz not null default now(),
  period_start timestamptz not null,
  period_end   timestamptz not null,
  category     text not null,
  code         text not null,
  description  text not null,
  count        bigint not null,
  sample       jsonb
);
create index ef_errors_period on ef_errors (period_start);
create index ef_errors_sample on ef_errors (code, description, period_start desc);

-- +goose Down
drop table ef_errors;
