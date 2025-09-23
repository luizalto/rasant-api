# ===================== Admin: Teste de Evento CAPI =====================
@app.get("/admin/test")
def admin_test_event(
    token: str,
    event_name: str = Query("ViewContent", description="ViewContent | AddToCart | Purchase | Lead ..."),
    event_source_url: str = Query("https://example.com/produto"),
    event_id: Optional[str] = Query(None, description="Se não enviar, usa um ID baseado no timestamp"),
    fbp: Optional[str] = Query(None, description="Ex: fb.1.1690000000.1234567890"),
    fbc: Optional[str] = Query(None, description="Ex: fb.1.1690000000.AbcDefFbclid"),
    client_ip: str = Query("1.2.3.4"),
    user_agent: str = Query("Mozilla/5.0 (Test)"),
    value: float = Query(0.0, description="Valor do evento (Purchase/AddToCart)"),
    currency: str = Query("BRL"),
    content_type: str = Query("product"),
    cat: Optional[str] = Query(None, description="content_category"),
    event_time: Optional[int] = Query(None, description="Unix epoch; se ausente usa agora")
):
    # Auth
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    now_ts = int(time.time())
    evt_time = int(event_time) if event_time else now_ts
    eid = event_id or f"test-{now_ts}"

    user_data = build_user_data_for_meta(client_ip, user_agent, fbp, fbc)
    custom_data = {
        "currency": currency,
        "value": value,
        "content_type": content_type
    }
    if cat:
        custom_data["content_category"] = cat

    resp = send_fb_event(
        event_name=event_name,
        event_id=eid,
        event_source_url=event_source_url,
        user_data=user_data,
        custom_data=custom_data,
        event_time=evt_time
    )
    return {"ok": True, "sent": {"event_name": event_name, "event_id": eid, "time": evt_time}, "response": resp}
