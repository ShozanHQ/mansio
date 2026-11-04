# A demo whose second block fails

```bash
curl -fsS "$MANSIO_URL/healthz" >/dev/null
```

```bash
echo ok
false-command-for-the-check
echo not reached
```
