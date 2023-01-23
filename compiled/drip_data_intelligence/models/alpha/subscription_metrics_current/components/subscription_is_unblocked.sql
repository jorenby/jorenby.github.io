select
    id as subscription_id,
    trust_status in ('trusted', 'untrusted', 'super_trusted') AS is_unblocked
from
    "dumps"."dev"."subscriptions"