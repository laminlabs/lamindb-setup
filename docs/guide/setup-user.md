# Sign up and log in

```
import lamindb_setup as ln_setup
```

## Sign up

```{code}

ln_setup.signup("testuser1@lamin.ai")  # CLI: lamin signup testuser1@lamin.ai
```

This will generate a password and cache both email and password in your `~/.lamin` directory.

📧 You will also receive a confirmation email with a link to choose your user handle and complete the sign-up.

:::{dropdown} Error messages

If you try to sign up again, you'll see an error message:

```

User already exists! Please login instead: `lamin login`.

```

If you did not confirm the signup email, you'll see:

```

RuntimeError: It seems you already signed up with this email. Please click on the link in the confirmation email that you should have received from lamin.ai.

```

Depending on timing, you might see a `429 Too Many Requests` error.
:::

## Log in

You can log in with either email or handle:

```
ln_setup.login("testuser1@lamin.ai")  # CLI: lamin login testuser1@lamin.ai
ln_setup.login("testuser1")  # CLI: lamin login testuser1
```

If you don't have a cached password in your environment, you need to pass it to the `login()` function:

```{code}
ln_setup.login("<email>", password="<password>")  # CLI: lamin login <email> --password <password>
```
