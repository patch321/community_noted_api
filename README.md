# community_noted_api

## Setup

```bash
docker build -t community_noted_api .
```

## Run

```bash
docker run -p 8000:8000 community_noted_api
```

## Push to GCP

```bash
docker tag community_noted_api gcr.io/community-noted/community_noted_api
docker push gcr.io/community-noted/community_noted_api
```
