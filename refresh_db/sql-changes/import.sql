update ps_configuration
set value = 'localhost'
where name in ('PS_SHOP_DOMAIN', 'PS_SHOP_DOMAIN_SSL');

update ps_configuration
set value = 0
where name in ('PS_SSL_ENABLED');


update ps_shop_url
set domain = 'localhost', domain_ssl='localhost'
where id_shop = 1 and id_shop_url=1;