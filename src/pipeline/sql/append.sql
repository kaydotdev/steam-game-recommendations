/* 
    Some game entities are dropped during the data cleaning phase, while their recommendations still remain.
    To prevent foreign key violations all orphaned recommendations are also dropped
*/
DELETE FROM public."RecommendationsAppend"
WHERE app_id NOT IN (SELECT app_id FROM public."Games");

-- Syncing data from `append` tables into the `master` tables
INSERT INTO public."Users" (userprofile, products)
SELECT userprofile, products
FROM public."UsersAppend"
ON CONFLICT (userprofile)
DO UPDATE SET
    products = EXCLUDED.products;

INSERT INTO public."Games" (app_id, title, description, tags, date_release, rating, positive_ratio, user_reviews, price_final, price_original, discount, win, mac, linux, steam_deck)
SELECT app_id, title, description, tags, date_release, rating, positive_ratio, user_reviews, price_final, price_original, discount, win, mac, linux, steam_deck
FROM public."GamesAppend"
ON CONFLICT (app_id)
DO UPDATE SET
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    tags = EXCLUDED.tags,
    date_release = EXCLUDED.date_release,
    rating = EXCLUDED.rating,
    positive_ratio = EXCLUDED.positive_ratio,
    user_reviews = EXCLUDED.user_reviews,
    price_final = EXCLUDED.price_final,
    price_original = EXCLUDED.price_original,
    discount = EXCLUDED.discount,
    win = EXCLUDED.win,
    mac = EXCLUDED.mac,
    linux = EXCLUDED.linux,
    steam_deck = EXCLUDED.steam_deck;

INSERT INTO public."Recommendations" (app_id, userprofile, date, is_recommended, helpful, funny, hours)
SELECT app_id, userprofile, date, is_recommended, helpful, funny, hours
FROM public."RecommendationsAppend"
ON CONFLICT (app_id, userprofile, date)
DO UPDATE SET
    is_recommended = EXCLUDED.is_recommended,
    helpful = EXCLUDED.helpful,
    funny = EXCLUDED.funny,
    hours = EXCLUDED.hours;

-- Removing temporary `append` tables
DROP TABLE IF EXISTS public."UsersAppend";
DROP TABLE IF EXISTS public."GamesAppend";
DROP TABLE IF EXISTS public."RecommendationsAppend";

