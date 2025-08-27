const sign_in_btn = document.querySelector("#sign-in-btn");
const sign_up_btn = document.querySelector("#sign-up-btn");
const container = document.querySelector(".container");

sign_up_btn.addEventListener('click', () =>{
    container.classList.add("sign-up-mode");
});

sign_in_btn.addEventListener('click', () =>{
    container.classList.remove("sign-up-mode");
});
const signUpForm = document.querySelector("#signUpForm");

signUpForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  const fullname = signUpForm.fullname.value.trim();
  const email = signUpForm.email.value.trim();
  const fonction = signUpForm.fonction.value.trim();
  const password = signUpForm.password.value;
  const confirmPassword = signUpForm.confirmPassword.value;

  if (!email.endsWith("@anp.org.ma")) {
    alert("L’e-mail doit se terminer par @anp.org.ma");
    return;
  }

  if (password !== confirmPassword) {
    alert("Les mots de passe ne correspondent pas.");
    return;
  }

  try {
    const response = await fetch("http://localhost:5000/api/signup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fullname, email, fonction, password }),
    });

    const data = await response.json();

    if (response.ok) {
      alert(data.message);
      container.classList.remove("sign-up-mode");
      signUpForm.reset();
    } else {
      alert(data.error || "Erreur lors de l’inscription.");
    }
  } catch (error) {
    console.error("Erreur réseau :", error);
    alert("Erreur réseau ou serveur.");
  }
});
const signInForm = document.querySelector("#signInForm");

signInForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  const email = signInForm.querySelector('input[type="text"]').value.trim();
  const password = signInForm.querySelector('input[type="password"]').value;

  try {
    const response = await fetch("http://localhost:5000/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });

    const data = await response.json();

    if (response.ok) {
      alert(`Bienvenue ${data.user.fullname}`);
       localStorage.setItem("fullname", data.user.fullname);
      window.location.href = "/dashboard.html";

    } else {
      alert(data.error || "Erreur de connexion.");
    }
  } catch (error) {
    console.error("Erreur réseau :", error);
    alert("Erreur serveur.");
  }
});
