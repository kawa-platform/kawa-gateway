package io.jonasg.kawa.http.users;

/// The body of `GET /admin/setup`.
///
/// @param required whether the first admin user still has to be created through `POST /admin/setup`
record SetupView(boolean required) {
}
