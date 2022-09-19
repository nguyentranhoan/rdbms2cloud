create or replace function trigger_set_timestamp() returns trigger
    language plpgsql
as
$$
BEGIN
  NEW.updated_at = cast(extract(epoch from current_timestamp) as BigInt);
  NEW.year = cast(extract(year from current_timestamp) as BigInt);
  NEW.month = cast(extract(year from current_timestamp) as BigInt);
  NEW.day = cast(extract(year from current_timestamp) as BigInt);
  RETURN NEW;
END;
$$;

alter function trigger_set_timestamp() owner to postgres;

CREATE TRIGGER set_timestamp
    before update
    on public.customer
    for each row
execute procedure trigger_set_timestamp();
