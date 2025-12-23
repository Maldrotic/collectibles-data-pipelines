-- Aggregated order metrics by day
select
  date_trunc('day', ordered_at) as order_date,
  count(distinct order_id) as total_orders,
  count(distinct customer_id) as unique_customers,
  sum(quantity) as total_cards_sold,
  sum(price_cents) / 100.0 as total_revenue_dollars
  avg(price_cents) / 100.0 as avg_order_value_dollars
from marts.fct_orders
where order_status not in ('ORDER_STATUS_CANCELLED', 'ORDER_STATUS_REFUNDED', 'ORDER_STATUS_UNSPECIFIED')
group by 1
order by 1 desc;


-- Top 10 selling cards by revenue
select
  c.sku,
  c.card_name,
  c.sport,
  c.player_name,
  c.parallel,
  COUNT(distinct o.order_id) as total_orders,
  sum(o.quantity) as total_cards_sold,
  sum(o.price_cents) / 100.0 as total_revenue_dollars,
  avg(o.price_cents) / 100.0 as avg_order_value_dollars
from marts.fct_orders o
join marts.dim_card c on o.card_key = c.card_key
where o.order_status not in ('ORDER_STATUS_CANCELLED', 'ORDER_STATUS_REFUNDED', 'ORDER_STATUS_UNSPECIFIED')
group by 1, 2, 3, 4, 5
order by total_revenue_dollars desc
limit 10;

-- Top customer segments (based on order count)
with customer_orders as (
  select
    customer_id,
    count(distinct order_id) as order_count,
    sum(price_cents) / 100.0 as total_revenue_dollars
  from marts.fct_orders
  where order_status not in ('ORDER_STATUS_CANCELLED', 'ORDER_STATUS_REFUNDED', 'ORDER_STATUS_UNSPECIFIED')
  group by 1
)
select
  case
    when order_count = 1 then '1 order'
    when order_count between 2 and 5 then '2-5 orders'
    when order_count between 6 and 10 then '6-10 orders'
    else '10+ orders'
  end as customer_segment,
  count(*) as customer_count,
  avg(total_revenue_dollars) as avg_lifetime_value_dollars,
  sum(total_revenue_dollars) as total_segment_revenue_dollars
from customer_orders
group by 1
order by customer_count desc;
