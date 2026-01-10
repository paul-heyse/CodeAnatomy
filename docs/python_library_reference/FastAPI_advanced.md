# FastAPI Advanced Reference (v0.110+)

## Dependency Injection and Dependencies

FastAPI’s dependency injection system uses the `Depends` function (or `Security` for security-specific flows) to declare “dependable” callables that run before your path function and supply values to it[\[1\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=)[\[2\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=But%20when%20you%20want%20to,Depends). The preferred modern style is to use Python 3.10’s `Annotated` for dependency parameters, e.g. `user: Annotated[User, Depends(get_current_user)]`, which makes the dependency explicit and avoids confusion with multiple parameters[\[3\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=Tip)[\[4\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=async%20def%20read_items%28commons%3A%20Annotated,if%20commons.q). FastAPI will call the dependency for you (do not call it manually) and inject its return value into your endpoint function[\[5\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Depends%28dependency%3DNone%2C%20). By default, each unique dependency is **called at most once per request** and its result **cached** for any other dependents (`use_cache=True` by default)[\[6\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=). You can override this with `use_cache=False` to force repeated calls if needed[\[6\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=), though caching is usually desirable.

**Nested and Sub-Dependencies:** Dependencies can depend on other dependencies (sub-dependencies). FastAPI will resolve the entire dependency graph – for example, a `get_db_session` dependency might yield a database session, and a `get_current_user` dependency might itself use `Depends(get_db_session)` to query the DB for an authenticated user. These run in a **topological order**, with sub-dependencies executed first and their return values threaded into dependent functions. If the same dependency is needed in multiple places in the graph (e.g. the same `get_db_session` used in multiple sub-dependencies), it will still only execute once per request due to caching[\[6\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=). Dependencies can be **declared globally** for all routes (via `app.dependencies`) or applied at the router or path level; global/router dependencies run for every request, often used for things like rate limiting or authentication enforcement[\[7\]](https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/#:~:text=,Security)[\[8\]](https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/#:~:text=Advanced%20User%20Guide%20,100).

**Classes as Dependencies:** You can use classes with `Depends` to maintain state or configuration. A class can be provided instead of a function – FastAPI will instantiate it (calling its `__init__`) and use the instance as the dependency result. If the class’s `__init__` has parameters, they become sub-dependencies automatically. For example, a class `CommonQueryParams` with an `__init__(q: str = None, limit: int = 100)` can be declared as `params: Annotated[CommonQueryParams, Depends(CommonQueryParams)]` and FastAPI will call `CommonQueryParams(q, limit)` using request query parameters[\[9\]\[10\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=class%20CommonQueryParams%3A%20def%20__init__,q%20%3D%20q). For more flexibility, a class instance can be made “callable” by defining `__call__`. FastAPI will then call the instance’s `__call__` on each request, allowing the class to behave like a function with internal state. This pattern is useful for **parameterized dependencies** – e.g. creating an instance `checker = FixedContentQueryChecker("keyword")` and using `Depends(checker)` to check if the query contains that keyword. In this case, FastAPI invokes `checker.__call__(...)` for each request[\[11\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=def%20__call__,fixed_content%20in%20q%20return%20False)[\[12\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=In%20this%20case%2C%20this%20,your%20path%20operation%20function%20later). The class’s `__init__` runs only once (when creating the instance), so use it to fix configuration (like the keyword to check), and put request-specific logic in `__call__`[\[13\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=In%20this%20case%2C%20this%20,your%20path%20operation%20function%20later)[\[14\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=return%20%7B).

**Security Dependencies:** The `Security` function works like `Depends` but adds support for **OAuth2 scopes** in OpenAPI. In most cases you can use `Depends` for auth, but if a dependency should enforce scopes, use `Security(dep, scopes=["..."])`. For example: `current_user: Annotated[User, Security(get_current_user, scopes=["admin"])]` will require that the `get_current_user` dependency (perhaps using an `OAuth2PasswordBearer` token) ensures the token has the `"admin"` scope[\[2\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=But%20when%20you%20want%20to,Depends)[\[15\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=match%20at%20L557%20current_user%3A%20Annotated,current_user.username). The dependency can receive a `SecurityScopes` object to inspect required scopes. FastAPI’s built-in security utilities (like `OAuth2PasswordBearer`) integrate with this mechanism.

**Yield Dependencies (Context Managers):** FastAPI supports dependencies defined as generators using `yield`, allowing setup and teardown. For example, a DB session dependency can open a session, `yield` it for use in the path function, then commit/close in the code after the yield. Such a dependency is declared like:

    def get_session() -> Generator[Session, None, None]:
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

Any code after `yield` runs after the response is sent (for request-scoped dependencies)[\[16\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Dependencies%20with%20,Technical%20Details%C2%B6). This is ideal for releasing resources. **Important:** If you catch exceptions in a yield dependency (using `try/except` around the yield), you *must re-raise* the exception to propagate errors properly[\[17\]](https://fastapi.tiangolo.com/release-notes/#:~:text=,Dependencies%20with%20yield%20and%20except)[\[18\]](https://fastapi.tiangolo.com/release-notes/#:~:text=def%20my_dep,SomeException%3A%20raise). As of FastAPI 0.110.0, failing to raise inside an except block will swallow the error and skip exception handlers[\[17\]](https://fastapi.tiangolo.com/release-notes/#:~:text=,Dependencies%20with%20yield%20and%20except), in line with normal Python context manager behavior.

**Dependency Scope:** By default, dependencies with `yield` finalize immediately after the path function returns (before sending response) – this is equivalent to scope `"function"`[\[19\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=). You can change a dependency’s scope to `"request"` to keep it alive until the response is fully sent (and **after** any streaming or background tasks)[\[19\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=). For example, `Depends(get_session, scope="request")` would keep the DB session open through response send, similar to FastAPI’s pre-0.106.0 behavior[\[20\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Before%20FastAPI%200,Handlers%20would%20have%20already%20run)[\[21\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=This%20was%20changed%20in%20FastAPI,to%20travel%20through%20the%20network). Use request scope carefully: holding resources longer than needed can reduce throughput. In most cases, the default (function scope) is sufficient and preferred for not holding resources during slow network sends[\[21\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=This%20was%20changed%20in%20FastAPI,to%20travel%20through%20the%20network).

**Sharing Dependencies with Background Tasks:** Note that after FastAPI v0.106.0, yield dependencies no longer stay open during background tasks (they close as soon as the main response is sent)[\[20\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Before%20FastAPI%200,Handlers%20would%20have%20already%20run)[\[22\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Additionally%2C%20a%20background%20task%20is,its%20own%20database%20connection). If you need to use a resource in a background task (e.g. DB session), it’s best to create a new resource inside the background task itself, or pass trivial data (like an identifier) to the task rather than an open DB connection[\[22\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Additionally%2C%20a%20background%20task%20is,its%20own%20database%20connection)[\[23\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=For%20example%2C%20instead%20of%20using,inside%20the%20background%20task%20function).

**Overriding Dependencies:** For testing or conditional logic, you can override dependencies. FastAPI’s `app.dependency_overrides` dict maps a dependency callable to an override callable[\[24\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=For%20these%20cases%2C%20your%20FastAPI,dict)[\[25\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=app.dependency_overrides). During request processing, if a dependency function is found in this dict, FastAPI calls the override instead. This is commonly used in tests to supply dummy data (e.g., overriding a `get_current_user` to return a fake user). Example in tests:

    app.dependency_overrides[get_current_user] = lambda: User(name="Test")

After tests, clear overrides with `app.dependency_overrides = {}` to avoid side effects[\[26\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=match%20at%20L574%20,dict). Dependencies can also be overridden at app include time by parameter `dependencies` or using `override_dependency` in *Advanced User Guide*, but the most direct method is the overrides dict or `TestClient` context override in tests.

## Background Tasks and Event Hooks

FastAPI provides **background tasks** for work that should happen after sending the response, and **event hooks** for startup/shutdown logic when the application lifespan starts or ends.

**BackgroundTasks Utility:** In a path function, you can declare a parameter `background_tasks: BackgroundTasks`. This is a special class that allows adding tasks to execute after the response is sent to the client[\[27\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=You%20can%20add%20middleware%20to,FastAPI%20applications)[\[28\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Technical%20Details). Use `background_tasks.add_task(callable, *args, **kwargs)` to schedule a function. For example:

    from fastapi import BackgroundTasks

    @app.post("/send-email/")
    def send_mail(recipient: str, background_tasks: BackgroundTasks):
        # enqueue the email send task
        background_tasks.add_task(send_welcome_email, recipient)
        return {"message": "Email scheduled"}

Here, `send_welcome_email(recipient)` will run in the background once the HTTP response is sent[\[28\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Technical%20Details). Background tasks are run after all middleware and after dependency teardown, in a separate thread or event loop slot. They are useful for sending emails, logging, or other tasks that need not block the response. Keep tasks idempotent and quick (they run in-process by default; for long-running jobs, a separate worker or task queue is recommended).

**Startup and Shutdown Events:** To run setup or teardown code for the app, use event hooks. Decorate functions with `@app.on_event("startup")` or `@app.on_event("shutdown")` to register them. For example, you might connect to a database on startup and disconnect on shutdown:

    @app.on_event("startup")
    async def on_startup():
        engine.connect()  # pseudocode: open DB connection

    @app.on_event("shutdown")
    async def on_shutdown():
        engine.disconnect()

These hooks run once per application lifecycle – when Uvicorn/Hypercorn starts and stops the app. If using multiple workers (Gunicorn), each worker process runs its own startup/shutdown. Startup events are also useful for initializations like loading ML models into memory, seeding caches, etc. They run before the app begins serving requests. By default, the TestClient will also trigger startup/shutdown events when used as a context manager (or you can explicitly trigger them via Lifespan, see testing section).

**Lifespan Context Manager (Advanced):** FastAPI (via Starlette 0.20+) supports a `lifespan` context function for application setup/teardown. Instead of separate decorators, you can define a single async generator function that yields on successful startup and executes finalization on exit[\[29\]\[30\]](https://fastapi.tiangolo.com/advanced/events/#:~:text=,shutdown). For example:

    app = FastAPI()

    @app.on_event("startup")
    async def startup():
        ...

    @app.on_event("shutdown")
    async def shutdown():
        ...

is functionally similar to:

    async def lifespan(app: FastAPI):
        # startup code here
        yield  # the point where the app runs
        # shutdown code here

    app = FastAPI(lifespan=lifespan)

Using `lifespan=func` can be cleaner for managing a context object that should be created at startup and used throughout the app, but either approach works. The **lifespan** approach can also yield an object (if needed) that dependencies might access via `request.app.state` (for instance, create a DB engine at startup and store it in `app.state.engine` for use in dependencies).

**Dependencies with Teardown:** Dependencies using `yield` act somewhat like mini event hooks on a per-request basis – their code after `yield` runs post-response. This can serve as a **teardown hook per request** (e.g., closing DB session, releasing a lock, etc.). One caveat: with **StreamingResponse** (discussed below), yield dependencies finalize *immediately* after the response is returned, even if streaming is still sending data. This means for truly long-lived streams, the teardown may occur before stream completion in FastAPI \<=0.117 (a known issue addressed in 0.118.0)[\[31\]](https://github.com/fastapi/fastapi/discussions/11444#:~:text=obj%20%3D%20MyClass,release)[\[32\]](https://github.com/fastapi/fastapi/discussions/11444#:~:text=async%20def%20rag_chat_async%28instance%3A%20Annotated,stream). A workaround is to manually manage resource closure (e.g., close DB session early if you don’t need it during streaming)[\[33\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=%40app.get%28,query)[\[34\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=def%20get_user%28user_id%3A%20int%2C%20session%3A%20Annotated,session.close).

## Streaming Responses and File Handling

For sending large content or realtime data, FastAPI offers streaming responses. The **StreamingResponse** class can take an iterator or async iterator and stream it as the response body, allowing you to send data without loading it all into memory[\[35\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=def%20generate_stream,1)[\[36\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=for%20ch%20in%20query%3A%20yield,1). For example:

    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse

    app = FastAPI()

    def generate_numbers():
        for i in range(1000):
            yield f"{i}\n"

    @app.get("/numbers")
    def stream_numbers():
        return StreamingResponse(generate_numbers(), media_type="text/plain")

This will start sending the response incrementally as the generator yields data. **Important:** When using StreamingResponse, the response is sent asynchronously; if you have dependencies with `yield`, by default their teardown code runs *before* the stream is fully consumed (since the path function returned)[\[37\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Before%20FastAPI%200,the%20internal%20server%20error%20handler)[\[32\]](https://github.com/fastapi/fastapi/discussions/11444#:~:text=async%20def%20rag_chat_async%28instance%3A%20Annotated,stream). If the stream requires a resource (e.g., DB cursor), consider using `Depends(..., scope="request")` to extend its lifetime through the response[\[19\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=) or manually manage resource closure as needed[\[38\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=But%20as%20,open%20while%20sending%20the%20response)[\[39\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=user%20%3D%20session,session.close) (e.g., closing a DB session as soon as you finish reading needed data, before yielding it).

**FileResponse:** To return files (such as images or documents) directly from disk, use `FileResponse`. It automatically handles setting the correct MIME type and file headers (including `Content-Disposition` for downloads). For example:

    from fastapi.responses import FileResponse

    @app.get("/download")
    def download_file():
        return FileResponse("/path/to/report.pdf", media_type="application/pdf", filename="report.pdf")

This will read the file in chunks and stream it to the client, setting headers so that the browser can download it. `FileResponse` uses an efficient file-handling under the hood (using Starlette’s `FileResponse` which can use file IO optimizations). Make sure to specify `filename=` if you want the download to have a friendly name. If the file is large, FileResponse is preferred over manually reading and returning bytes (which would load the whole file in memory).

**Use Cases for Generators/Iterators:** Generators can implement **Server-Sent Events** or any long polling solutions by yielding event data. By setting `media_type="text/event-stream"` and yielding properly formatted messages, you can push events to clients. Similarly, an async generator can be used if your data production involves `await` (wrap it in StreamingResponse; FastAPI will await it appropriately). Keep in mind that streaming responses do not have a content length, so the client will receive data as a chunked transfer. If you need to flush data promptly (e.g., SSE pings), ensure the generator yields periodically.

FastAPI also supports **Custom Response Classes** for streaming other protocols or use cases. For example, you could subclass Starlette’s `StreamingResponse` to tweak behavior. But common scenarios are covered by built-ins.

When streaming or sending files, **consider using background tasks** for any cleanup (like deleting a temporary file after sending). For instance, if you generate a file on disk to send and want to remove it later, you can attach a background task to delete the file after response is sent:

    @app.get("/generate-report")
    def report(background_tasks: BackgroundTasks):
        path = generate_report_pdf()  # your function
        background_tasks.add_task(os.remove, path)
        return FileResponse(path, filename="report.pdf")

This ensures the file cleanup happens post-response.

## Security and Authentication

FastAPI’s security utilities simplify common auth schemes like OAuth2 (JWT Bearer tokens), API keys, and HTTP Basic. They integrate with dependency injection to protect routes and provide user info to your code.

**OAuth2 with Password (Bearer JWT):** This is a common setup for token-based auth. FastAPI provides `OAuth2PasswordBearer` in `fastapi.security` to represent a “bearer token” dependency (usually JWT). You create one by specifying the token URL (the endpoint where clients get tokens). For example:

    from fastapi.security import OAuth2PasswordBearer
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", scopes={"admin": "Admin privileges", "user": "Regular user"})

Here, `tokenUrl="token"` means your app will have an `/token` path to get tokens (often using `OAuth2PasswordRequestForm` for username/password). You can optionally define scope names with descriptions[\[40\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=You%20can%20import%20,fastapi). The dependency `oauth2_scheme` yields a token string (if present). Use it in dependencies like:

    from jose import jwt, JWTError

    SECRET_KEY = "secret"
    ALGORITHM = "HS256"

    async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"})
        user_id: str = payload.get("sub")
        ...
        return User(...)

The `Depends(oauth2_scheme)` will ensure the function receives the `Authorization: Bearer <token>` header value (or raises 401 if missing). Your code then decodes the JWT (using e.g. PyJWT or python-jose) and verifies it[\[41\]\[42\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=payload%20%3D%20jwt.decode%28token%2C%20SECRET_KEY%2C%20algorithms%3D,username%20is%20None%3A%20raise%20credentials_exception). If invalid, raise `HTTPException(status_code=401)` with `"WWW-Authenticate": "Bearer"` header so that the client knows to provide auth[\[41\]\[42\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=payload%20%3D%20jwt.decode%28token%2C%20SECRET_KEY%2C%20algorithms%3D,username%20is%20None%3A%20raise%20credentials_exception). Once you retrieve the user (e.g. from DB or a user service), return it. This `get_current_user` can then be used as a dependency in path operations to secure them:

    @app.get("/users/me")
    def read_own_data(current_user: Annotated[User, Depends(get_current_user)]):
        return {"username": current_user.username}

If the user is not authenticated, this will return 401 before reaching the body of the function.

**Password Hashing:** Always store hashed passwords, not plaintext. FastAPI’s docs demonstrate using **PassLib** to hash and verify passwords[\[43\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=,Update%20the%20%2Ftoken%20path%20operation)[\[44\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=,91). For example, using PassLib’s `CryptContext`:

    from passlib.context import CryptContext
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    hashed = pwd_context.hash(plain_password)
    pwd_context.verify(plain_password, hashed)  # returns True if matches

Include password hashing in your user creation flow and verification in your token generation route (the `/token` route would verify the user’s password then issue a JWT). The docs also show `OAuth2PasswordRequestForm` dependency to easily parse form-encoded username and password from a request[\[45\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=from%20fastapi,PasswordHash%20from%20pydantic%20import%20BaseModel).

**OAuth2 Scopes:** As mentioned, you can protect routes with scopes. When defining `OAuth2PasswordBearer(tokenUrl="...", scopes={"scope_name": "Description", ...})`, the JWT should include a list of scopes (often in a claim like `"scopes"` or space-separated in `"scope"` depending on implementation). In the `get_current_user` dependency, you can access the required scopes via `SecurityScopes` if you accept a parameter `security_scopes: SecurityScopes` (FastAPI will inject it). Then:

    if security_scopes.scopes:  # non-empty list
        # e.g., ["admin"]
        token_scopes = payload.get("scopes", [])
        for scope in security_scopes.scopes:
            if scope not in token_scopes:
                raise HTTPException(..., detail="Not enough permissions")

If any required scope is missing, return 401 or 403 appropriately[\[2\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=But%20when%20you%20want%20to,Depends)[\[15\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=match%20at%20L557%20current_user%3A%20Annotated,current_user.username). In your path operation, use `Security(get_current_user, scopes=["desired_scope"])` so that the dependency knows what is required[\[15\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=match%20at%20L557%20current_user%3A%20Annotated,current_user.username). These scopes will appear in the automatic documentation, and the OAuth2 authorization UI in docs will allow requesting specific scopes.

**API Key Security:** For simpler APIs, you might use API keys passed via headers, query parameters, or cookies. You can implement this with dependencies as well. FastAPI provides `APIKeyHeader`, `APIKeyQuery`, etc. from `fastapi.security` to assist. For example:

    from fastapi.security import APIKeyHeader
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    async def get_api_key(api_key: Annotated[str | None, Depends(api_key_header)]):
        if api_key is None or api_key != VALID_API_KEY:
            raise HTTPException(401, detail="Invalid API Key")

Then use `Depends(get_api_key)` on routes that need the header. Setting `auto_error=False` means if the header is missing, it passes None to your function and you can handle it (returning 401). You could similarly use `APIKeyQuery(name="key")` for a query param.

**HTTP Basic Auth:** FastAPI supports HTTP Basic via `HTTPBasic` in `fastapi.security`. It will parse the `Authorization: Basic ...` header. Usage:

    from fastapi.security import HTTPBasic, HTTPBasicCredentials
    security = HTTPBasic()

    def get_current_user_basic(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
        correct_username = secrets.compare_digest(credentials.username, USERNAME)
        correct_password = secrets.compare_digest(credentials.password, PASSWORD)
        if not (correct_username and correct_password):
            raise HTTPException(401, detail="Invalid basic auth", headers={"WWW-Authenticate": "Basic"})
        return {"username": credentials.username}

`HTTPBasicCredentials` provides `.username` and `.password`. Note the use of `secrets.compare_digest` for safe comparison. Protect routes with `Depends(get_current_user_basic)`. Basic auth is simpler but less secure (credentials sent on every request encoded in base64); use HTTPS and consider more robust auth for production.

**CORS (Cross-Origin Resource Sharing):** If your API is consumed from browsers on a different domain, enable CORS. Use Starlette’s `CORSMiddleware` via FastAPI:

    from fastapi.middleware.cors import CORSMiddleware

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://example.com"],  # domains allowed
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-My-Custom-Header"]   # if you want clients to read custom headers
    )

This will automatically handle `OPTIONS` preflight requests and add appropriate CORS headers to responses. Adjust allowed origins, methods, and headers as needed. Using `"*"` for `allow_origins` will allow all domains (use with caution in production). **Note:** To allow browsers to see custom response headers, list them in `expose_headers` (the example above exposes a custom header)[\[46\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Tip)[\[47\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Keep%20in%20mind%20that%20custom,prefix).

**HTTPS Redirect & Security Headers:** If you want to enforce HTTPS, use `HTTPSRedirectMiddleware` which redirects all HTTP to HTTPS[\[48\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=)[\[49\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=from%20fastapi%20import%20FastAPI%20from,httpsredirect%20import%20HTTPSRedirectMiddleware). Add it via `app.add_middleware(HTTPSRedirectMiddleware)`. To guard against Host header attacks, consider `TrustedHostMiddleware`, which you can configure with allowed host patterns (e.g. your domain)[\[50\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=)[\[51\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=app%20%3D%20FastAPI). It will return 400 if a request comes with an unknown Host. GZip compression can be enabled with `GZipMiddleware` to automatically compress responses for clients that support it[\[52\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=) (you can specify minimum size, etc.). FastAPI includes these via Starlette and you add them as needed.

## Middleware and ASGI Integration

**Middleware** functions wrap the processing of every request/response[\[53\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=You%20can%20add%20middleware%20to,FastAPI%20applications)[\[54\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=,Then%20it%20returns%20the%20response). A middleware can run code **before** reaching the path operation (e.g. logging, authentication) and **after** the response is produced (e.g. adding headers, trailing logging). You can create a middleware with the `@app.middleware("http")` decorator on an async function that receives `request` and a `call_next` function[\[55\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=To%20create%20a%20middleware%20you,on%20top%20of%20a%20function)[\[56\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=from%20fastapi%20import%20FastAPI%2C%20Request). For example:

    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)  # process the request down to route
        duration = time.perf_counter() - start
        response.headers["X-Process-Time"] = f"{duration:0.4f}"
        return response

This example times each request and injects a header showing how long it took[\[57\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=app%20%3D%20FastAPI)[\[58\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=from%20fastapi%20import%20FastAPI%2C%20Request). The `call_next(request)` forwards the request to the next middleware (or path operation if none left) and returns the response[\[59\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=The%20middleware%20function%20receives%3A). You can modify the response before returning it.

**Middleware Execution Order:** If you have multiple middlewares, they execute in the order added for the request (first added = outermost, runs first) and in reverse order for the response. FastAPI’s built-in middlewares (like exception handlers, CORSMiddleware if added, etc.) will wrap around your custom ones depending on add order. For example, if you add CORS middleware and then a custom one, the custom one’s `call_next` will invoke the CORS middleware, which then invokes the app. Generally, order of `app.add_middleware` calls defines the wrapping order.

**Dependency Interaction:** Middleware runs outside of the dependency system. Any code in a middleware sees the raw `request` before dependencies, and after the response is created (but before dependencies’ teardown finalize). Notably, if you have dependencies with `yield`, their teardown happens **after** the middleware’s *before* logic and **before** middleware’s after logic completes[\[60\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Technical%20Details). And background tasks run last, after all middleware complete[\[60\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Technical%20Details). Be cautious: if you modify the request body/stream in a middleware, it might affect downstream operations (since the request can only be read once).

**Custom ASGI Middleware Classes:** FastAPI is built on Starlette and supports any ASGI middleware. Many third-party middlewares (for sessions, compression, etc.) expect you to wrap the app. Instead of manually wrapping (`app = SomeMiddleware(app)`), FastAPI provides `app.add_middleware` which correctly preserves exception handling order[\[61\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=But%20FastAPI%20,custom%20exception%20handlers%20work%20properly)[\[62\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=app.add_middleware%28UnicornMiddleware%2C%20some_config%3D). For example, to add a third-party ASGI middleware:

    app.add_middleware(UnicornMiddleware, some_config="rainbow")

FastAPI will instantiate `UnicornMiddleware(app, some_config="rainbow")` internally[\[63\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=For%20that%2C%20you%20use%20,in%20the%20example%20for%20CORS). Any standard ASGI middleware (one that takes an ASGI app and returns a new ASGI app) can be used. This includes Starlette’s own middlewares, which FastAPI re-exports (e.g., `fastapi.middleware.TrustedHostMiddleware`).

If you need more control (like acquiring resources per request, or catching exceptions), you can also subclass Starlette’s `BaseHTTPMiddleware`:

    from starlette.middleware.base import BaseHTTPMiddleware

    class CustomMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            # pre-processing
            try:
                response = await call_next(request)
                return response
            finally:
                # post-processing (even if exception)
                ...

Register with `app.add_middleware(CustomMiddleware)`. You can also write a pure ASGI middleware by defining an `__call__` that accepts `scope, receive, send` and handling the protocol – but for HTTP, using BaseHTTPMiddleware or the decorator is simpler.

**Common Middleware Examples:**

- *Logging:* You might log each request’s path and method in a middleware, and log the response status and duration. Use `request.url.path` and `request.method` before calling next, then after response use `response.status_code`. This ensures all requests are logged uniformly.

- *Error Monitoring:* A middleware can catch unhandled exceptions not caught by FastAPI exception handlers. However, since FastAPI already converts exceptions like HTTPException to responses, typically you use exception handlers or the logger. That said, you could wrap `call_next` in a try/except to log exceptions to an external service (like Sentry) then re-raise or return an error response.

- *Metrics:* For gathering metrics (like request count, latency, etc.), a middleware is ideal. As shown, you can time requests and record metrics. For per-endpoint metrics, you can use `request.url.path` or `request.scope["endpoint"]` to identify the handler.

- *Correlation/Request IDs:* To trace requests through logs, a middleware can check for an incoming request ID header (like `X-Request-ID`) or generate one (UUID). You can then attach it to the response (set the same header) and possibly store it in `request.state` for access in the endpoint. Example:

<!-- -->

    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = rid
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response

Now the `request_id` is accessible in path operations via `request.state.request_id` and also returned to the client, enabling end-to-end tracing.

**Starlette Request/State:** The `Request` object (from `fastapi import Request`) is a Starlette request. You can attach arbitrary data to `request.state` inside dependencies or middleware and it will persist for the life of that request[\[64\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=As%20FastAPI%20is%20based%20on,can%20use%20any%20ASGI%20middleware). For example, an auth middleware could decode a token and set `request.state.user`, so that deeper in the app (even in endpoints or dependencies) you could retrieve `request.state.user` without recalculating it. This pattern can complement or replace using global dependency for user extraction.

**Third-Party ASGI Middleware:** There are many reusable ASGI middlewares: e.g., `Starlette's SessionMiddleware` for server-side sessions, authentication middlewares, rate limiters (like `slowapi`), etc. As long as they follow ASGI spec, use `app.add_middleware`. Keep in mind that some middleware (like session or CSRF) might expect certain order or that you use them with particular integration (e.g., Starlette’s SessionMiddleware works with its `session` in request.state). Always consult their documentation. FastAPI doesn’t treat them differently – they will work since FastAPI is just Starlette under the hood[\[64\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=As%20FastAPI%20is%20based%20on,can%20use%20any%20ASGI%20middleware).

Finally, **WebSocket middleware**: ASGI middlewares will also wrap WebSocket connections (unless specifically coded to only handle http). If you add a global middleware, it may see WS scopes too. Ensure your middleware checks `request.scope["type"]` if it should only handle http. For example, the logging example might check `if request.scope["type"] == "http": ...`. Some Starlette middlewares automatically skip non-http.

## Advanced Routing and OpenAPI

FastAPI builds on Starlette’s routing to support **mounting sub-applications, routers, and versioning strategies**.

**APIRouter and Modular Design:** Instead of defining all routes on a single `app`, you can create multiple `APIRouter` instances (from `fastapi` module) to organize routes by functionality or version. For example:

    from fastapi import APIRouter

    items_router = APIRouter(prefix="/items", tags=["items"])

    @items_router.get("/")
    async def list_items():
        ...

    @items_router.post("/")
    async def create_item(item: Item):
        ...

    app.include_router(items_router)

Here, all routes in `items_router` will be prefixed with `/items` and tagged “items” in docs. You can mount routers at different prefixes (even nested routers). This is the preferred way to structure larger applications (often each router in a separate file/module)[\[65\]](https://fastapi.tiangolo.com/tutorial/security/get-current-user/#:~:text=%2A%20%20CORS%20%28Cross,Advanced%20User%20Guide). It simplifies **versioning**: you could create `v1_router = APIRouter(prefix="/v1")` and include feature routers under it, then later create `v2_router` with updated endpoints. Including both in the app would expose `/v1/...` and `/v2/...` APIs.

**Mounting Sub-Applications:** You can mount a wholly separate ASGI app or FastAPI app at a specific path. Use `app.mount("/path", sub_app)`. For example, if you have a second FastAPI app `admin_app` for administrative UI, you can mount it:

    admin_app = FastAPI()
    # define routes on admin_app ...
    app.mount("/admin", admin_app)

Now requests to `/admin/*` go to `admin_app` and others go to the main app[\[66\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=app.mount%28). The mounted app can even be a different framework, like a Starlette app or even a WSGI app via Starlette’s `WSGIMiddleware`[\[67\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=,111)[\[68\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=,120). Each sub-app has its own routes and middleware. FastAPI will also aggregate the OpenAPI docs by default: a request to main `/docs` shows paths from sub-apps too, with their prefixes[\[69\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=Mount%20the%20sub). If you want separate documentation, you might run sub-apps separately instead of mount.

**Static Files:** A common use of `mount` is serving static files. Starlette provides `StaticFiles`. For example:

    from starlette.staticfiles import StaticFiles
    app.mount("/static", StaticFiles(directory="static"), name="static")

This will serve files at URLs like `/static/myfile.png` from the `./static` directory. It’s efficient (serves directly from disk). Remember that in production behind a CDN or separate web server, you might serve static files outside of FastAPI, but for simplicity this works. If you mount static files with `name="static"`, you can use `url_for("static", path="myfile.png")` to get URLs in templates.

**Dynamic Route Generation:** You can programmatically add routes using `app.add_api_route()`. FastAPI (via Starlette) allows building routes on the fly. For example, you might read a config or database at startup and create routes accordingly. `app.add_api_route("/dynamic/{name}", endpoint=dynamic_handler, methods=["GET"])` will add a GET route. The `endpoint` can be a function; if it’s not decorated, you should ensure to include proper type hints on parameters for FastAPI to do validation. Alternatively, generate APIRouter/Include at startup. Dynamic route generation is powerful but use carefully to avoid conflict and keep docs meaningful.

**Route Dependencies and Overrides:** You can attach dependencies to **router groups**. When including a router via `include_router`, you can pass a list of dependencies that apply to all its routes. For instance, if an entire router (say `/admin` endpoints) should require admin user, you can do:

    app.include_router(admin_router, prefix="/admin", dependencies=[Depends(check_admin)])

This adds `check_admin` dependency (which could raise 403 if not admin) to every route in `admin_router`[\[70\]](https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/#:~:text=,Security). It’s a convenient way to enforce security or policies on a group of routes without repeating in each route decorator.

**OpenAPI Schema Customization:** FastAPI auto-generates an OpenAPI 3.1 schema (as of recent versions) for your APIs. You can customize it or extend it in several ways:

- *OpenAPI Method Override:* You can supply a custom function to `app.openapi`. By default, `app.openapi()` generates the schema dict. You can assign your own function to `app.openapi = my_custom_openapi` to modify the schema (e.g., to add global security requirements, inject more info, etc.). In `my_custom_openapi()`, likely call the original generator `schema = get_openapi(...); ...; return schema`. The FastAPI docs have a “Extending OpenAPI” recipe on this[\[71\]](https://fastapi.tiangolo.com/vi/reference/fastapi/#:~:text=FastAPI%20class%20,return%20func%20return%20decorator)[\[72\]](https://fastapi.tiangolo.com/ru/tutorial/middleware/#:~:text=%D0%A1%D0%BE%D0%B7%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5%20middleware%C2%B6,).

- *Docs URLs:* You can set `docs_url` and `redoc_url` in `FastAPI(...)` to change or disable the documentation UIs. For example `FastAPI(docs_url="/documentation", redoc_url=None)` would host Swagger UI at /documentation and disable ReDoc.

- *Operation IDs and Tags:* You can specify `operation_id` (unique name) for each path using `@app.get(..., operation_id="myCustomId")` if needed for client SDKs. You can also group operations with `tags=[...]` which reflect in the docs. Use tags to organize large APIs by sections.

- *Additional Responses:* By default, FastAPI documents the successful response and common errors (422 validation). If you want to document other responses (e.g., 404 or 403), use the `responses` parameter on the decorator. Example:

<!-- -->

    @app.get("/item/{id}", responses={404: {"description": "Not found"}})
    def read_item(id: str):
        item = get_item(id)
        if not item:
            raise HTTPException(404, "Not found")
        return item

This will add 404 to the OpenAPI schema with that description[\[73\]](https://fastapi.tiangolo.com/advanced/additional-responses/#:~:text=%2A%20%20Custom%20Response%20,102)[\[74\]](https://fastapi.tiangolo.com/advanced/additional-responses/#:~:text=Advanced%20Security%20,114). You can even provide a response model in the responses dict for non-2xx codes.

- *Schema Documentation:* You can add descriptive metadata to models using Pydantic (like Field descriptions, examples) which propagate to OpenAPI. FastAPI also supports Markdown in doc strings for descriptions in OpenAPI via the `description` parameter on path decorators or in the `Field` metadata.

**Versioning Strategies:** FastAPI doesn’t impose a versioning method, but common approaches:

- Path versioning: Include the version in the URL (e.g. `/v1/items`, `/v2/items`). This can be done by creating separate routers for each version. For large differences, you might maintain separate FastAPI apps or routers and mount them (`app.mount("/v2", v2_app)`).

- Subdomain versioning: Use Starlette’s `Mount` with host parameter (or a proxy in front) to direct traffic by subdomain (advanced usage, not covered in basic docs).

- Header or Query versioning: Not automatic; you’d manually inspect a header in a dependency and route accordingly, or simply handle in logic. OpenAPI can document multiple versions via different paths or separate schemas per version if needed.

**Custom APIRoute Class:** If you need to modify how route logic works globally (like logging every request in a custom way or modifying request/response objects in a central place beyond middleware), you can subclass `fastapi.routing.APIRoute` and tell FastAPI to use it. For example, a custom APIRoute could time each request or catch exceptions differently. This is an advanced pattern: you set `app.router.route_class = YourAPIRouteClass` or supply `route_class` when including a router. Inside that class, override `await run(endpoint_function)` to wrap the execution.

**OpenAPI Callbacks/Webhooks:** FastAPI supports defining callback endpoints (if your API needs the client to host an endpoint that your API calls back). This is an OpenAPI feature and can be added via `app.add_api_route` with `include_in_schema=False` for the callback path and then adding the callback schema to your main path’s OpenAPI. This is quite advanced and usually not needed unless implementing complex webhook flows; refer to the “OpenAPI Callbacks” docs for details[\[73\]](https://fastapi.tiangolo.com/advanced/additional-responses/#:~:text=%2A%20%20Custom%20Response%20,102).

In summary, FastAPI’s routing is flexible: use routers for structure, mount apps or static files for segmentation, and tweak OpenAPI as needed for documentation accuracy. Always ensure each path operation’s (endpoint’s) name is unique (function names are used for operationId generation unless explicitly set) to avoid any conflicts in generated schema.

## WebSockets

FastAPI supports WebSocket endpoints for real-time two-way communication, building on Starlette’s WebSocket support. Define a WebSocket route with `@app.websocket("/path")` decorator (note: use `.websocket` instead of `.get`/`.post`)[\[75\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,Try%20the%20WebSockets%20with%20dependencies)[\[76\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,121). The endpoint function should accept a `WebSocket` parameter (from `fastapi import WebSocket`). Example:

    from fastapi import WebSocket

    @app.websocket("/ws/chat")
    async def websocket_chat(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                data = await ws.receive_text()
                await ws.send_text(f"Echo: {data}")
        except WebSocketDisconnect:
            print("Client disconnected")

In this simple echo example, the server accepts the connection, then enters a loop to receive messages and send responses. The `WebSocket` object has methods `receive_text()`, `receive_json()`, etc. as well as `send_text()`, `send_json()`, and `send_bytes()`. It’s an async interface and will raise `WebSocketDisconnect` when the client disconnects, which you can catch to perform any cleanup.

**Connection Management:** In a real chat or real-time app, you typically need to manage multiple clients. You can maintain a set or list of active `WebSocket` connections. For example:

    active_connections: list[WebSocket] = []

    @app.websocket("/ws/chat")
    async def chat_ws(ws: WebSocket):
        await ws.accept()
        active_connections.append(ws)
        try:
            while True:
                data = await ws.receive_text()
                # broadcast to all connections
                for conn in active_connections:
                    await conn.send_text(f"{data}")
        except WebSocketDisconnect:
            active_connections.remove(ws)

This will broadcast incoming messages to all connected clients. Note that this simple approach sends messages sequentially to each client – for large numbers of clients, you might want to dispatch tasks for sending to avoid one slow client back-pressuring others. Also, consider locking if you manipulate a shared list of connections from multiple tasks (though each connection runs in its own task naturally).

**WebSocket Routes and Dependencies:** You can use dependencies (`Depends`) with WebSocket endpoints as well[\[77\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,119)[\[78\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,OpenAPI%20Webhooks). For instance, if you want to authenticate the WebSocket handshake, you could have:

    @app.websocket("/ws/secure")
    async def secure_ws(websocket: WebSocket, token: Annotated[str, Depends(oauth2_scheme)]):
        # perform token verification similar to HTTP
        user = decode_token(token)
        if not user:
            # If not allowed, you must close the websocket; HTTPException won’t auto-handle in WS
            await websocket.close(code=1008)  # policy violation or auth failure code
            return
        await websocket.accept()
        ...

FastAPI will try to resolve dependencies before allowing the websocket function to run. If a dependency raises an `HTTPException`, FastAPI will catch it and close the WebSocket with error code 1008 (policy violation) by default, as there is no concept of HTTP response for websockets[\[2\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=But%20when%20you%20want%20to,Depends). You should handle closure logic accordingly. Alternatively, do a manual auth at the start of the `websocket_chat` by reading headers or query params via `websocket.headers` or `websocket.query_params`.

**Handling Disconnections:** Always wrap your receive/send loop in a try/except for `WebSocketDisconnect`. When a client disconnects (either closing connection or network issue), `await ws.receive_*()` will raise `WebSocketDisconnect`. In the except, you can clean up (e.g., remove connection from list, notify others if needed). WebSocketDisconnect has a `.code` attribute for the close code if you need to inspect.

**Backpressure and Async Considerations:** `WebSocket.receive_*` will block until a message is received or connection closes. If you want to handle multiple connections concurrently (which is typical), each connection handler is an independent coroutine, so as long as you `await` properly, multiple clients can be served. If you have tasks that shouldn’t block message handling (like periodic broadcasts), consider using `asyncio.create_task` inside the loop or an external background task.

**Broadcasting to All or Groups:** The example above shows naive broadcasting to all connected. For larger scale, you might maintain channels or groups of websockets to send relevant messages. This can be done at app level by keeping track of which socket is in which room/channel.

**Scaling WebSockets:** Remember that WebSocket connections are long-lived. Each connection ties up one coroutine on the server (and potentially one open TCP socket). Ensure your server (Uvicorn, etc.) is configured with appropriate workers or consider using an event-driven worker with sufficient resources. If deploying with Gunicorn, you typically use the Uvicorn workers (`--worker-class uvicorn.workers.UvicornWorker`); they support websockets. With multiple worker processes, a single client will stick to one process – you cannot easily share WebSocket state between processes without external pubsub (like Redis). For truly large scale websockets (thousands of clients, needing cross-process communication), consider using a background pub-sub or a dedicated websocket server.

FastAPI itself handles WebSockets similar to any ASGI app. You might also use libraries like `starlette.websockets.WebSocketRoute` to define complex patterns or subclass `WebSocketEndpoint` class for a stateful connection handler (with on_connect, on_receive, on_disconnect methods), but the function approach is usually sufficient.

**Testing WebSockets:** In tests, you can use `TestClient`’s `websocket_connect` context to simulate websocket clients. For example, `with TestClient(app).websocket_connect("/ws/chat") as websocket:` then use `websocket.send_text()` and `websocket.receive_text()` in test code.

## Testing and Test Clients

FastAPI is designed to be easily testable. You can run the app in tests using **Starlette’s TestClient** or an HTTP client like HTTPX in ASGI mode.

**Synchronous Tests with TestClient:** The simplest way is to use `fastapi.testclient.TestClient`, which wraps your app and allows making requests as if from an HTTP client. Example with Pytest:

    from fastapi.testclient import TestClient
    from .main import app  # your FastAPI app

    client = TestClient(app)

    def test_read_main():
        res = client.get("/")
        assert res.status_code == 200
        assert res.json() == {"message": "Hello World"}

The `TestClient` is built on **requests** (or HTTPX in sync mode internally) and will run your app in a separate thread/event loop, handling async endpoints transparently. When using `TestClient`, any startup events will run when the client is created (or when entering a context manager) and shutdown events when it’s closed. If you need events to run, best practice is:

    with TestClient(app) as client:
        # startup events have run
        res = client.get("/...")
        ...
    # exiting context triggers shutdown events

Or you can instantiate once per session if tests are read-only.

**Asynchronous Tests with AsyncClient:** You might prefer to write async tests (using `pytest.mark.asyncio` or `pytest.mark.anyio`) especially if testing async dependencies or performance. The HTTPX library provides `AsyncClient` that can mount the ASGI app directly[\[79\]](https://app-generator.dev/docs/technologies/fastapi/testing.html#:~:text=generator,assert%20response). Example:

    import pytest
    from httpx import AsyncClient

    @pytest.mark.anyio
    async def test_async_endpoint():
        async with AsyncClient(app=app, base_url="http://test") as ac:
            resp = await ac.get("/items")
        assert resp.status_code == 200

Here, `AsyncClient` from HTTPX runs requests against the ASGI app in-process (no network). The `base_url` is required (it can be any value; `"http://test"` is commonly used)[\[79\]](https://app-generator.dev/docs/technologies/fastapi/testing.html#:~:text=generator,assert%20response). This approach is excellent for testing async behaviors and is non-blocking. Note that if your app has startup events, HTTPX’s client **does not automatically trigger them**. To ensure startup events, you might need to manually call `app.router.startup()` before tests or use `LifespanManager` from `starlette.testclient`. However, as of recent Starlette, providing `app=app` should handle lifespan (Starlette will handle lifespan events on first request if not explicitly managed). To be safe in async tests, you can do:

    from starlette.testclient import LifespanManager

    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            ...

This ensures the `startup` event has run before making requests, and shutdown will run after.

**Dependency Overrides in Tests:** As mentioned, you can override dependencies via `app.dependency_overrides` in tests[\[24\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=For%20these%20cases%2C%20your%20FastAPI,dict). This is useful to mock out external calls (database, HTTP to third party, etc.). For example, to test a route that depends on `get_current_user`, override it to return a known test user:

    app.dependency_overrides[get_current_user] = lambda: User(name="Test", roles=["admin"])
    res = client.get("/admin-area")
    assert res.status_code == 200

Alternatively, FastAPI provides `TestClient` context manager to temporarily override dependencies using a `override_dependency` method (not widely used; the dict approach is straightforward). Always clear overrides after (especially if tests share the app instance) to avoid bleed-over. One pattern is to override in a fixture and then reset in fixture teardown.

**Mocking External Services:** If your code calls external web services, you can avoid hitting them in tests by either dependency injection or by using libraries like `responses` or `respx` (for HTTPX). For instance, if you have a dependency `get_payment_client` that returns an API client, override it in tests to return a dummy object whose methods you control. Or if calling directly inside route code, use `monkeypatch` in Pytest to replace the function or method with a stub. FastAPI’s design encourages moving such calls into dependencies so they can be easily overridden.

**Testing WebSockets:** Use `TestClient.websocket_connect`. Example:

    def test_websocket_echo():
        with TestClient(app) as client:
            with client.websocket_connect("/ws/chat") as websocket:
                websocket.send_text("hello")
                data = websocket.receive_text()
                assert data == "Echo: hello"

The context manager yields a `WebSocketTestSession` which has `send_text`, `receive_text`, etc. If testing broadcast logic, you might open multiple websocket connections (perhaps in different threads or async tasks). Alternatively, use `httpx.AsyncClient` with `ws_connect` if more control needed.

**Testing Lifespan Events:** If using `@app.on_event`, TestClient as context ensures they run. For more explicit testing of startup/shutdown behaviors, you can call event handlers directly if they’re accessible, or use Starlette’s `LifespanManager` in an async test as described. FastAPI also includes docs on testing lifespan events specifically[\[29\]\[30\]](https://fastapi.tiangolo.com/advanced/events/#:~:text=,shutdown).

**Using TestClient with multiple workers or multi-thread:** Note that TestClient by default runs the app in the same process asynchronously. This means if your app uses an in-memory database or global state, it’s consistent within that TestClient. If you use `multiprocessing` or run tests in parallel processes, they won’t share state (which is usually fine or even desired). For thread-safety, ensure your app code doesn’t have unprotected globals being modified.

**Performance Testing and Advanced:** For load testing, you might not use TestClient (since it’s synchronous). Instead, deploying the app and using an external tool (locust, vegeta, etc.) might be more appropriate. But for correctness and logic, the above methods suffice.

**Pytest anyio plugin:** Using `@pytest.mark.anyio` allows you to run both sync and async tests seamlessly. With `anyio`, even `client.get()` calls inside an async test will work by running them in a threadpool. However, you might just stick to sync tests for simple cases and only use async tests when truly needed.

## Async Patterns and Concurrency

FastAPI (based on ASGI) is inherently asynchronous-friendly, but integrating async code (like database access) and controlling concurrency requires some patterns.

**Async Database Access:** Modern libraries like **SQLAlchemy 1.4+/2.0** support async. For example, using SQLAlchemy 2.x in async mode:

    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker

    engine = create_async_engine("postgresql+asyncpg://user:pass@host/db")
    AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def get_db():
        async with AsyncSessionLocal() as session:
            yield session

Here `get_db()` is an async generator dependency that yields an `AsyncSession`. In your path ops, you can use `session: Annotated[AsyncSession, Depends(get_db)]` and then use `await session.execute(...)` for queries. FastAPI will await the dependency since it’s async. When the request is done, the session context manager closes the session (committing or rolling back as appropriate). This pattern ensures **non-blocking** DB operations. Similar patterns exist for other async ORMs (Tortoise, GINO, etc.).

If using **MongoDB** with Motor, or Redis with an async client, you can also inject client instances via dependencies.

For **async I/O** (like calling external APIs), you can use `httpx.AsyncClient` inside path functions or dependencies. Make sure not to call blocking I/O (like regular file operations or heavy CPU work) in the async code; use threads (via `run_in_threadpool`) or background tasks for those to avoid blocking the event loop.

**Limiting Concurrency:** Sometimes you have resources that can only handle N concurrent accesses (e.g., an external API with rate limit, or a section of code that should be single-threaded). An `asyncio.Semaphore` or `asyncio.Lock` is useful here. Example:

    search_limit = asyncio.Semaphore(5)  # allow 5 concurrent calls

    async def external_search(query: str):
        async with search_limit:
            return await call_external_api(query)

By using a semaphore, if more than 5 coroutines enter `external_search`, they will wait until others finish. This pattern can throttle throughput to protect resources. You could implement this as a dependency too, but typically a global semaphore is fine (since it’s process-wide; with multiple worker processes, semaphore isn’t shared across them).

**Async Tasks and** `await` **inside Endpoints:** If you need to fire off concurrent sub-tasks within a single request (say, fetch data from multiple services in parallel), you can use `asyncio.create_task` or gather:

    task1 = asyncio.create_task(service_call1())
    task2 = asyncio.create_task(service_call2())
    result1 = await task1
    result2 = await task2

This will run the calls concurrently on the same event loop. Make sure to handle exceptions (if one fails, gather would raise; using `await asyncio.gather(task1, task2, return_exceptions=True)` is one way to capture all results without immediate exception propagation).

**Threadpool vs Async:** If you have CPU-bound work (like heavy computations or image processing), the async loop can’t help – consider running that in an external process or thread. Within FastAPI, you can use `starlette.concurrency.run_in_threadpool` to offload a blocking function to a threadpool so it doesn’t block the loop. For example:

    from starlette.concurrency import run_in_threadpool

    @app.get("/expensive")
    async def expensive_op():
        result = await run_in_threadpool(sync_expensive_function)
        return {"result": result}

This will let other requests be handled while that CPU-bound task runs in a thread. Note that GIL-bound CPU tasks in Python may not benefit much from threads; consider multi-process or specialized solutions if needed.

**Deployment Considerations:** For production, you often run uvicorn with multiple worker processes to handle more load. For example: `uvicorn myapp:app --host 0.0.0.0 --port 80 --workers 4`. Each worker is an independent process with its own event loop. If using Gunicorn, use the Uvicorn worker class as mentioned (e.g., `gunicorn -k uvicorn.workers.UvicornWorker -w 4 myapp:app`). Make sure to set appropriate timeout and keep-alive settings in Gunicorn for ASGI.

If your app uses a lot of long-lived connections (WebSockets or many slow streaming responses), a single process might handle thousands of coroutines, but you may need to adjust Uvicorn’s loop configuration or consider using `uvloop` for efficiency (Uvicorn will use uvloop if installed by default, which is recommended for speed).

**Lifespan and Startup DB Connections:** If you use an object that should persist through the app (like a DB engine or pool), create it on startup (in `on_event("startup")`) and store it in `app.state`. E.g.:

    @app.on_event("startup")
    async def startup():
        app.state.redis = await aioredis.create_redis_pool(...)

    @app.on_event("shutdown")
    async def shutdown():
        app.state.redis.close()
        await app.state.redis.wait_closed()

Then a dependency can use `request.app.state.redis` to get the pool and run commands. This avoids re-creating pools on every request.

**Async Gunicorn Workers vs Thread Workers:** Do not use Gunicorn thread workers for async code – always prefer async workers (like UvicornWorker). Threads with async frameworks can cause a lot of context-switch overhead and aren’t beneficial. Each UvicornWorker already can handle many concurrent requests via async.

**Testing Async Interactions:** When testing async code paths (like DB calls), use transactions rollbacks or test-specific databases to avoid persistent side effects. Libraries like `pytest-asyncio` or `pytest-anyio` help manage the event loop in tests properly.

**Limits and Timeouts:** Implement client-side timeouts (like with HTTPX’s timeout param) to avoid hanging forever on external calls. On server side, Uvicorn/Gunicorn can enforce timeouts on keep-alive and graceful shutdown, but there’s no built-in per-request timeout in FastAPI. You could implement one via dependency that uses `asyncio.wait_for` to time-limit certain operations, or at the reverse-proxy level.

**Long Running Tasks:** For tasks that truly should outlive the request (like lengthy processing that user will poll for result), consider using a task queue (Celery, RQ, etc.) or background thread that stores state in a database. While FastAPI’s background tasks run after response, they are still on the same server process – they are fine for short tasks (seconds, maybe a minute or two), but not for heavy CPU or long minutes-hour tasks (those should be offloaded to a worker service to not tie up server resources).

In summary, use Python async features to their fullest: prefer async DB/IO libraries, parallelize where possible with tasks, guard critical sections with semaphores/locks, and offload blocking code to threads or processes. This helps FastAPI handle high concurrency efficiently[\[19\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=).

## Response Modeling and Custom Responses

FastAPI leverages Pydantic models to validate and document responses. The `response_model` parameter in route decorators defines the model used to serialize the output and generate OpenAPI schema[\[80\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Example)[\[81\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=%40app.get%28,return%20commons). For example:

    class ItemOut(BaseModel):
        name: str
        description: str

    @app.get("/items/{id}", response_model=ItemOut)
    def read_item(id: str):
        item = db.get(id)
        return item  # could be a dict or Pydantic model

FastAPI will ensure the returned item matches the schema of `ItemOut` – fields will be validated, extraneous fields removed (unless you set `response_model_include/exclude` to tweak), and the model is used in the docs. If your path returns a Pydantic model instance, FastAPI will serialize it to dict via `.dict()` (Pydantic v1) or `.model_dump()` (Pydantic v2) under the hood.

**Advanced** `response_model` **Usage:** You can use complex types like `List[ItemOut]` or `Dict[str, ItemOut]` in response_model to indicate a collection. FastAPI will handle those (the OpenAPI will show an array of ItemOut, etc.). If you want to **exclude fields** in the response (for example, you have an internal field you don’t want to expose), you can either define a separate Pydantic model for output that omits it, or use `response_model_exclude={"internal_field"}` in the decorator. Similarly `response_model_include` can whitelist fields. You can also set `response_model_by_alias=True/False` to control whether field aliases are used in output.

By default, Pydantic model output is serialized with `jsonable_encoder`, converting datetime, UUID, etc. to json-friendly types. If performance is critical and you want to skip validation, you can set `response_model=None` (or just not use it) and return raw JSONable data – but then you lose automatic documentation of the schema and checking.

**Custom Response Classes:** FastAPI provides `fastapi.Response` (and subclasses) for when you need full control. For instance, returning a simple plaintext or HTML without Pydantic:

- `from fastapi import Response`: You can do `return Response(content="OK", media_type="text/plain", status_code=200)` for a basic response. This content is not validated or documented (unless you manually specify in responses). Use this when you already have the exact content (like a JSON string or HTML string) and don’t need FastAPI’s serialization.

- `HTMLResponse`, `PlainTextResponse`, `JSONResponse`, etc.: Convenient subclasses that set media type. E.g. `return HTMLResponse("<h1>Hello</h1>")` to return HTML content. Or `JSONResponse({"key": "value"}, headers={"X-Header": "value"})` to send raw JSON with custom header. Normally you don’t need `JSONResponse` because returning a dict or Pydantic model and setting `response_model` covers it, but it can be useful if you want to bypass Pydantic for performance or return something like decimal or date that Pydantic might not handle by default (though jsonable_encoder covers many types).

- `RedirectResponse`: for HTTP redirects (3xx status) with a URL.

- `StreamingResponse` and `FileResponse`: (covered earlier) for streaming and file data.

You can also define your own `starlette.responses.Response` subclass if needed.

**Setting Headers and Cookies:** You can add headers by returning a Response object (like in the above JSONResponse example). If you use the normal path return (like returning a dict), and you need to set a header, you have two options: (1) Declare a `Response` parameter in your path function (from fastapi import Response) – FastAPI will pass in the actual response object it’s creating. You can then do `response.headers["X-Foo"] = "Bar"`. This is useful if you still want to return a Pydantic model but set a header as a side effect. (2) Use `@app.get(..., response_class=JSONResponse)` and construct the Response manually with headers. The first approach is typically easier:

    @app.post("/users")
    def create_user(user: UserIn, response: Response):
        uid = save_user_to_db(user)
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/users/{uid}"
        return {"id": uid, "name": user.name}

Here we set status code and header on the Response object. FastAPI by default uses `JSONResponse` for JSONable returns, so our dict becomes JSON, and we effectively just mutated the response.

For cookies, you can similarly use `Response.set_cookie("cookie-name", "value", httponly=True, max_age=... )` within the route via the injected Response. Or return a Response directly with cookies set.

**Status Code Control:** You can set a default status code in the decorator: e.g. `@app.post("/items", status_code=201)`. If the endpoint returns normally, that status will be used. If you return an `HTTPException` or raise one, that status overrides for error. If you manually set `response.status_code` in the function as above, that also overrides. FastAPI’s `status` module provides convenient names like `status.HTTP_201_CREATED`. Using `status_code` parameter is recommended for fixed success codes. If you have logic that sometimes returns different success codes, you might need to either use `Response` manual control or return a `Response` object for those cases. For example, an endpoint that either creates (201) or finds existing (200) might do:

    if created:
        return JSONResponse(new_item.dict(), status_code=201)
    else:
        return {"id": item.id, "name": item.name}  # will be 200 by default

**Response Examples in OpenAPI:** You can use the `responses` parameter to provide example bodies or different schemas for errors (as discussed). If you have multiple response models for different outcomes, note that the `response_model` parameter only documents the main success case. Use `responses={404: {"model": ErrorMessageModel}}` to document an error response shape, for instance.

**Alternate Response Bodies (same status):** If you sometimes return completely different data types for the same endpoint (not just optional fields, but totally different schemas), you might use `Union` types for `response_model`. FastAPI can combine Pydantic models using `Union[ModelA, ModelB]` to indicate either schema may be returned. The OpenAPI output will use a oneOf with both schemas. Validation will allow either. Internally, FastAPI will try to parse your actual return into one of the models. This can work if models are distinct enough. But it's often simpler to just design your API to have a consistent response schema (possibly with a type field to distinguish variants).

**StreamingResponse as response_model:** Typically when returning `StreamingResponse` or `FileResponse`, you set `response_class=...` in the decorator, and you don’t use `response_model` (since it’s not a JSON response). These bypass Pydantic serialization. The docs note that if you return a Response subclass, FastAPI will not perform any data conversion – you are responsible for providing properly encoded bytes/stream.

**Using** `Annotated` **for Response Bodies:** In Python 3.10+, you can also use `Annotated` in the return type for dependency injection or validation context, but usually it’s not needed. Instead, rely on `response_model`.

**Content Negotiation:** FastAPI by default sends JSON for dicts and Pydantic models (via `application/json`). If you need to return different content types based on `Accept` header, you could inspect `request.headers.get("accept")` in your endpoint and choose a response class accordingly. FastAPI’s routing does not do content negotiation automatically beyond what you specify. But you can define the same path with different `response_class` and `dependencies=[Depends(...)]` checking Accept – not common though. Usually, one writes a single endpoint and returns different `Response` types based on some parameter.

**HTTPException and Error Handling:** When you raise `HTTPException(status_code, detail="...")`, FastAPI intercepts it and returns an error JSON with `{"detail": "..."}` by default. You can customize exception handlers for more complex error payloads (e.g., include code, message). For instance, to override the 404 handler:

    from fastapi.responses import JSONResponse

    @app.exception_handler(StarletteHTTPException)
    async def custom_http_exception(request, exc):
        return JSONResponse(
            {"error": {"code": exc.status_code, "message": exc.detail}},
            status_code=exc.status_code
        )

This way, all HTTPExceptions (which 404 is one) will use that format. Document this via the `responses` param if needed so clients know the schema.

In short, FastAPI’s response system covers common needs with `response_model` for validation/serialization[\[80\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Example)[\[81\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=%40app.get%28,return%20commons) and allows escaping that layer with explicit Response classes when fine-grained control is required. Always test the actual responses (using TestClient) to ensure they contain exactly what you expect (headers, status, body) especially when mixing manual Response with dependency injection.

## Typed Routing and Data Validation with Pydantic v2

FastAPI heavily utilizes Python type hints for request parsing and validation. Recent Python versions (3.10+) and Pydantic v2 introduce new ways to declaratively describe your request/response data.

**Using** `Annotated` **for Parameters:** Traditionally, to add extra validation or metadata to a parameter (like query params) you would do: `def route(limit: int = Query(gt=0, lt=100))`. Now, using `Annotated`, you can separate the type and its FastAPI-specific info:

    from typing import Annotated
    from fastapi import Query

    @app.get("/items")
    def list_items(limit: Annotated[int, Query(gt=0, lt=100)] = 10):
        ...

This says `limit` is an int with a Query constraint 0 \< limit \< 100, default 10. `Annotated[T, X]` allows multiple metadata too – e.g., `Annotated[int, Query(...), SomeOtherMetadata(...)]`. FastAPI internally will interpret those metadata classes (like Query, Path, Body, etc.) and apply them. The docs recommend using Annotated where possible for clarity[\[3\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=Tip)[\[4\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=async%20def%20read_items%28commons%3A%20Annotated,if%20commons.q), and indeed it’s the way forward with Pydantic v2 (since `Field` and `Param` functions integrate with Annotated).

**Pydantic v2 Changes:** FastAPI v0.100+ supports Pydantic v2, which has some differences. Notably, Pydantic v2’s BaseModel uses `model_dump()` instead of `.dict()`, and validation uses `pydantic.ValidationError`. FastAPI abstracts most differences, but if you directly manipulate models you should use the new methods. Also, Pydantic v2 introduced `Annotated` support for models too – you can now use `Annotated` types within BaseModel to attach validators (though that’s more of a Pydantic usage detail). Pydantic v2 also merges `Json` type handling and has a more strict handling of data conversion (e.g., int-\>str conversions might require `StrictStr` for no conversion). In FastAPI, one impact is that complex nested models or uncommon types might need custom validators if Pydantic can’t coerce them (like `Decimal` no longer converts from str by default without a validator).

**Custom Data Types and Validation:** You can create custom classes that integrate with Pydantic by implementing the `pydantic.GetterDict` or (in v2) the `__get_pydantic_core_schema__` protocol. But an easier method: use **pydantic BaseModel for complex things** or **pydantic con\* types** (like `conint`, `constr`) for constrained types. For example:

    from pydantic import conint
    PositiveInt = conint(gt=0)
    @app.get("/")
    def demo(val: PositiveInt):
        return val

This will ensure `val` is \> 0. If invalid, a 422 is returned with error details.

For truly custom validation, you can add dependency functions that validate something. E.g., to validate a request header value is in some set:

    def validate_header(x_custom: Annotated[str, Header()]):
        if x_custom not in ALLOWED:
            raise HTTPException(400, "Invalid header")
        return x_custom

    @app.get("/data")
    def get_data(_, hdr: Annotated[str, Depends(validate_header)]):
        ...

This uses a dependency purely to perform a validation and pass the value through (or raise error).

**Using Dataclasses:** FastAPI can also accept Python 3.7+ dataclasses as request bodies (it will convert them to Pydantic models behind scenes). By adding `@dataclass` to a class and using it as a type in your endpoint, FastAPI will treat it similar to a BaseModel. This can be convenient if you prefer dataclasses, though they lack some of Pydantic’s features (no validation on their own beyond type hints).

**Custom Field Types:** If you have a custom class (like an enum or a special ID type), you can make it work with FastAPI by either making it a Pydantic-compatible type (implement `__str__` or `__repr__` or a validator). Enums are automatically supported (they become choice fields). For example, an `Enum` subclass can be used as a query or path parameter, and FastAPI will constrain input to the enum values[\[80\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Example)[\[81\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=%40app.get%28,return%20commons).

**Future of** `Annotated`**:** The use of `Annotated` in function signature (for dependencies and parameter validation) is now the recommended style (the documentation even repeats "prefer Annotated version if possible"[\[3\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=Tip)). It makes the signature’s type more truthful (since the actual value your function gets is of that type, not a `Depends` or whatever). Also multiple annotations: If you had both a dependency and an alias or validation, you can combine them. For example, `user_id: Annotated[int, Path(gt=0), Depends(parse_token)]` would theoretically both parse a token to get a user_id and validate it’s \> 0 (though realistically you might separate concerns).

**Typed Responses with Annotated:** Although not common, you could hint your function returns `ItemOut` to indicate that. FastAPI doesn’t use that for anything (it uses `response_model`), but for your clarity and static analysis it could help. In future, maybe a mypy plugin or similar could check that return type vs response_model align.

**Type Decorators:** This could refer to creating your own declarative shortcuts. For instance, you might want a custom type that always comes from a certain header. While not built-in, you can emulate it:

    class ClientID(str):
        @classmethod
        def __get_pydantic_core_schema__(cls, source, handler):
            # define a validator if needed
            return str

    def ClientIDHeader():
        return Header(alias="X-Client-ID")

    @app.get("/stats")
    def stats(client_id: Annotated[ClientID, Depends(ClientIDHeader())]):
        ...

However, this example is a bit convoluted. Simpler is just `client_id: Annotated[str, Header(alias="X-Client-ID")]`.

Some have created custom dependency classes that parse a token and yield a specific dataclass (for example, a `TokenData` type that includes scopes and user). Callable classes and `__call__` (as covered) are a way to create *stateful* or parameterized dependencies.

In summary, “typed routing” means you rely on Python types and standard library as much as possible to define your API, letting FastAPI/Pydantic handle the heavy lifting of parsing and validation. Embrace `Annotated` for clarity, use Pydantic models for complex schemas (ensuring they reflect exactly what you accept/return), and exploit Pydantic’s new features in v2 (like `field_validators`, `model_validator` decorators for complex validations across fields, etc.). FastAPI v0.110+ fully supports Pydantic v2, so you can use the `@field_validator` (formerly `@validator`) and `@model_config` (to set json schema options) in your models if needed.

Lastly, always test the edge cases: e.g., required query params, validation errors, etc., to ensure your type hints and validators are working as intended. FastAPI will produce clear error messages in the HTTP 422 response with details on which field failed and why, thanks to Pydantic’s error system, which is helpful for debugging and for clients to understand usage[\[80\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Example).

[\[1\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=) [\[2\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=But%20when%20you%20want%20to,Depends) [\[5\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Depends%28dependency%3DNone%2C%20) [\[6\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=) [\[15\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=match%20at%20L557%20current_user%3A%20Annotated,current_user.username) [\[19\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=) [\[40\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=You%20can%20import%20,fastapi) [\[80\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=Example) [\[81\]](https://fastapi.tiangolo.com/reference/dependencies/#:~:text=%40app.get%28,return%20commons) Dependencies - Depends() and Security() - FastAPI

<https://fastapi.tiangolo.com/reference/dependencies/>

[\[3\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=Tip) [\[4\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=async%20def%20read_items%28commons%3A%20Annotated,if%20commons.q) [\[9\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=class%20CommonQueryParams%3A%20def%20__init__,q%20%3D%20q) [\[10\]](https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/#:~:text=class%20CommonQueryParams%3A%20def%20__init__,q%20%3D%20q) Classes as Dependencies - FastAPI

<https://fastapi.tiangolo.com/tutorial/dependencies/classes-as-dependencies/>

[\[7\]](https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/#:~:text=,Security) [\[8\]](https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/#:~:text=Advanced%20User%20Guide%20,100) [\[70\]](https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/#:~:text=,Security) Global Dependencies - FastAPI

<https://fastapi.tiangolo.com/tutorial/dependencies/global-dependencies/>

[\[11\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=def%20__call__,fixed_content%20in%20q%20return%20False) [\[12\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=In%20this%20case%2C%20this%20,your%20path%20operation%20function%20later) [\[13\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=In%20this%20case%2C%20this%20,your%20path%20operation%20function%20later) [\[14\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=return%20%7B) [\[16\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Dependencies%20with%20,Technical%20Details%C2%B6) [\[20\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Before%20FastAPI%200,Handlers%20would%20have%20already%20run) [\[21\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=This%20was%20changed%20in%20FastAPI,to%20travel%20through%20the%20network) [\[22\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Additionally%2C%20a%20background%20task%20is,its%20own%20database%20connection) [\[23\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=For%20example%2C%20instead%20of%20using,inside%20the%20background%20task%20function) [\[33\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=%40app.get%28,query) [\[34\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=def%20get_user%28user_id%3A%20int%2C%20session%3A%20Annotated,session.close) [\[35\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=def%20generate_stream,1) [\[36\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=for%20ch%20in%20query%3A%20yield,1) [\[37\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=Before%20FastAPI%200,the%20internal%20server%20error%20handler) [\[38\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=But%20as%20,open%20while%20sending%20the%20response) [\[39\]](https://fastapi.tiangolo.com/advanced/advanced-dependencies/#:~:text=user%20%3D%20session,session.close) Advanced Dependencies - FastAPI

<https://fastapi.tiangolo.com/advanced/advanced-dependencies/>

[\[17\]](https://fastapi.tiangolo.com/release-notes/#:~:text=,Dependencies%20with%20yield%20and%20except) [\[18\]](https://fastapi.tiangolo.com/release-notes/#:~:text=def%20my_dep,SomeException%3A%20raise) Release Notes - FastAPI

<https://fastapi.tiangolo.com/release-notes/>

[\[24\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=For%20these%20cases%2C%20your%20FastAPI,dict) [\[25\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=app.dependency_overrides) [\[26\]](https://fastapi.tiangolo.com/advanced/testing-dependencies/#:~:text=match%20at%20L574%20,dict) Testing Dependencies with Overrides - FastAPI

<https://fastapi.tiangolo.com/advanced/testing-dependencies/>

[\[27\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=You%20can%20add%20middleware%20to,FastAPI%20applications) [\[28\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Technical%20Details) [\[46\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Tip) [\[47\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Keep%20in%20mind%20that%20custom,prefix) [\[53\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=You%20can%20add%20middleware%20to,FastAPI%20applications) [\[54\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=,Then%20it%20returns%20the%20response) [\[55\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=To%20create%20a%20middleware%20you,on%20top%20of%20a%20function) [\[56\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=from%20fastapi%20import%20FastAPI%2C%20Request) [\[57\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=app%20%3D%20FastAPI) [\[58\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=from%20fastapi%20import%20FastAPI%2C%20Request) [\[59\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=The%20middleware%20function%20receives%3A) [\[60\]](https://fastapi.tiangolo.com/tutorial/middleware/#:~:text=Technical%20Details) Middleware - FastAPI

<https://fastapi.tiangolo.com/tutorial/middleware/>

[\[29\]](https://fastapi.tiangolo.com/advanced/events/#:~:text=,shutdown) [\[30\]](https://fastapi.tiangolo.com/advanced/events/#:~:text=,shutdown) Lifespan Events - FastAPI

<https://fastapi.tiangolo.com/advanced/events/>

[\[31\]](https://github.com/fastapi/fastapi/discussions/11444#:~:text=obj%20%3D%20MyClass,release) [\[32\]](https://github.com/fastapi/fastapi/discussions/11444#:~:text=async%20def%20rag_chat_async%28instance%3A%20Annotated,stream) Dependencies with yield not working with StreamingResponse · fastapi fastapi · Discussion \#11444 · GitHub

<https://github.com/fastapi/fastapi/discussions/11444>

[\[41\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=payload%20%3D%20jwt.decode%28token%2C%20SECRET_KEY%2C%20algorithms%3D,username%20is%20None%3A%20raise%20credentials_exception) [\[42\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=payload%20%3D%20jwt.decode%28token%2C%20SECRET_KEY%2C%20algorithms%3D,username%20is%20None%3A%20raise%20credentials_exception) [\[43\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=,Update%20the%20%2Ftoken%20path%20operation) [\[44\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=,91) [\[45\]](https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/#:~:text=from%20fastapi,PasswordHash%20from%20pydantic%20import%20BaseModel) OAuth2 with Password (and hashing), Bearer with JWT tokens - FastAPI

<https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/>

[\[48\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=) [\[49\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=from%20fastapi%20import%20FastAPI%20from,httpsredirect%20import%20HTTPSRedirectMiddleware) [\[50\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=) [\[51\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=app%20%3D%20FastAPI) [\[52\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=) [\[61\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=But%20FastAPI%20,custom%20exception%20handlers%20work%20properly) [\[62\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=app.add_middleware%28UnicornMiddleware%2C%20some_config%3D) [\[63\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=For%20that%2C%20you%20use%20,in%20the%20example%20for%20CORS) [\[64\]](https://fastapi.tiangolo.com/advanced/middleware/#:~:text=As%20FastAPI%20is%20based%20on,can%20use%20any%20ASGI%20middleware) Advanced Middleware - FastAPI

<https://fastapi.tiangolo.com/advanced/middleware/>

[\[65\]](https://fastapi.tiangolo.com/tutorial/security/get-current-user/#:~:text=%2A%20%20CORS%20%28Cross,Advanced%20User%20Guide) Get Current User - FastAPI

<https://fastapi.tiangolo.com/tutorial/security/get-current-user/>

[\[66\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=app.mount%28) [\[67\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=,111) [\[68\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=,120) [\[69\]](https://fastapi.tiangolo.com/advanced/sub-applications/#:~:text=Mount%20the%20sub) Sub Applications - Mounts - FastAPI

<https://fastapi.tiangolo.com/advanced/sub-applications/>

[\[71\]](https://fastapi.tiangolo.com/vi/reference/fastapi/#:~:text=FastAPI%20class%20,return%20func%20return%20decorator) FastAPI class - Tiangolo.com

<https://fastapi.tiangolo.com/vi/reference/fastapi/>

[\[72\]](https://fastapi.tiangolo.com/ru/tutorial/middleware/#:~:text=%D0%A1%D0%BE%D0%B7%D0%B4%D0%B0%D0%BD%D0%B8%D0%B5%20middleware%C2%B6,) Middleware (Промежуточный слой) - FastAPI

<https://fastapi.tiangolo.com/ru/tutorial/middleware/>

[\[73\]](https://fastapi.tiangolo.com/advanced/additional-responses/#:~:text=%2A%20%20Custom%20Response%20,102) [\[74\]](https://fastapi.tiangolo.com/advanced/additional-responses/#:~:text=Advanced%20Security%20,114) Additional Responses in OpenAPI - FastAPI

<https://fastapi.tiangolo.com/advanced/additional-responses/>

[\[75\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,Try%20the%20WebSockets%20with%20dependencies) [\[76\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,121) [\[77\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,119) [\[78\]](https://fastapi.tiangolo.com/advanced/websockets/#:~:text=,OpenAPI%20Webhooks) WebSockets - FastAPI

<https://fastapi.tiangolo.com/advanced/websockets/>

[\[79\]](https://app-generator.dev/docs/technologies/fastapi/testing.html#:~:text=generator,assert%20response) How to test a FastAPI Project - A Practical Guide - App Generator

<https://app-generator.dev/docs/technologies/fastapi/testing.html>
