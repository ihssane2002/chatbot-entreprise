const express = require("express");
const mongoose = require("mongoose");
const bcrypt = require("bcryptjs");
const cors = require("cors");
const { spawn, exec } = require("child_process");
const path = require("path");
const fs = require("fs/promises");
const fsSync = require("fs"); // Pour existsSync
const multer = require("multer");

const app = express();
app.use(cors());
app.use(express.json());

// === Connexion MongoDB ===
mongoose.connect("mongodb://localhost:27017/anp_users", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log("MongoDB connecté"))
.catch((err) => console.error("Erreur MongoDB :", err));

// === Modèle utilisateur ===
const userSchema = new mongoose.Schema({
  fullname: String,
  email: { type: String, unique: true, required: true },
  fonction: String,
  password: { type: String, required: true },
});
const User = mongoose.model("User", userSchema);

// === Créer dossier uploads s’il n’existe pas ===
const uploadsDir = path.join(__dirname, "uploads");
if (!fsSync.existsSync(uploadsDir)) {
  fsSync.mkdirSync(uploadsDir);
}

// === Multer configuration (à faire une seule fois !) ===
const upload = multer({ dest: uploadsDir });

// === Servir les fichiers HTML/CSS/JS depuis interface_utilisateur/ ===
app.use(express.static(path.join(__dirname, "interface_utilisateur")));

// === Inscription ===
app.post("/api/signup", async (req, res) => {
  try {
    const { fullname, email, fonction, password } = req.body;

    if (!email.endsWith("@anp.org.ma")) {
      return res.status(400).json({ error: "Email doit se terminer par @anp.org.ma" });
    }

    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return res.status(400).json({ error: "Email déjà utilisé" });
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    const newUser = new User({ fullname, email, fonction, password: hashedPassword });
    await newUser.save();

    res.status(201).json({ message: "Inscription réussie" });
  } catch (error) {
    console.error("Erreur inscription:", error);
    res.status(500).json({ error: "Erreur serveur lors de l'inscription" });
  }
});

// === Connexion ===
app.post("/api/login", async (req, res) => {
  try {
    const { email, password } = req.body;

    const user = await User.findOne({ email });
    if (!user) {
      return res.status(400).json({ error: "Utilisateur non trouvé" });
    }

    const isValid = await bcrypt.compare(password, user.password);
    if (!isValid) {
      return res.status(400).json({ error: "Mot de passe incorrect" });
    }

    res.json({
      message: "Connexion réussie",
      user: { fullname: user.fullname, email: user.email, fonction: user.fonction },
    });
  } catch (error) {
    console.error("Erreur login:", error);
    res.status(500).json({ error: "Erreur serveur lors de la connexion" });
  }
});

// === API Query vers Python ===
app.post("/api/query", (req, res) => {
  const { question, history = [] } = req.body;
  if (!question) {
    return res.status(400).json({ error: "Question manquante" });
  }

  const scriptPath = path.join(__dirname, "..", "query_rag.py");
  const python = spawn("python", [scriptPath, question, JSON.stringify(history)]);

  let output = "";
  let errorOutput = "";

  python.stdout.on("data", (data) => {
    const text = data.toString();
    console.log("STDOUT Python :", text);
    output += text;
  });

  python.stderr.on("data", (data) => {
    console.error("STDERR Python :", data.toString());
    errorOutput += data.toString();
  });

  python.on("close", (code) => {
    console.log("Script Python terminé avec code:", code);
    if (code !== 0) {
      return res.status(500).json({ error: "Erreur d'exécution Python", details: errorOutput });
    }

    try {
      const responseJson = JSON.parse(output.trim());
      res.json(responseJson);
    } catch (err) {
      console.error("Erreur parsing JSON Python:", err);
      res.status(500).json({ error: "Erreur parsing JSON Python" });
    }
  });
});

// === Servir les fichiers PDF ===
app.use("/static/rapports", express.static(path.join(__dirname, "..", "modele_rag", "pdfs")));

// === Téléversement de fichiers PDF ===
app.post("/api/upload-pdf", upload.single("pdf"), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: "Aucun fichier envoyé." });

  const originalName = req.file.originalname;
  const tempPath = req.file.path;
  const finalPdfDir = path.join(__dirname, "..", "modele_rag", "pdfs");
  const targetPath = path.join(finalPdfDir, originalName);
  const scriptPath = path.join(__dirname, "..", "main.py");

  // Crée le dossier PDF s’il n’existe pas
  if (!fsSync.existsSync(finalPdfDir)) {
    fsSync.mkdirSync(finalPdfDir, { recursive: true });
  }

  // Fichier existe déjà ? => retraitement uniquement
  if (fsSync.existsSync(targetPath)) {
    console.log("Fichier déjà présent, on le traite à nouveau.");
    exec(`python "${scriptPath}"`, (error, stdout, stderr) => {
      if (error) {
        console.error("Erreur exec Python:", error);
        console.error("STDERR:", stderr);
        return res.status(500).json({
          error: "Erreur lors du traitement Python (fichier déjà existant).",
          details: stderr.toString(),
          output: stdout.toString(),
        });
      }

      console.log("Traitement terminé:", stdout);
      return res.json({ message: "PDF déjà existant mais retraité avec succès." });
    });
    return;
  }

  // Déplacement du fichier et traitement
  try {
    await fs.rename(tempPath, targetPath);

    exec(`python "${scriptPath}"`, (error, stdout, stderr) => {
      if (error) {
        console.error("Erreur exec Python:", error);
        console.error("STDERR:", stderr);
        console.error("STDOUT:", stdout);
        return res.status(500).json({
          error: "Erreur lors du traitement Python.",
          details: stderr.toString(),
          output: stdout.toString(),
        });
      }

      console.log("Traitement terminé:", stdout);
      res.json({ message: "PDF ajouté et traité avec succès." });
    });
  } catch (err) {
    console.error("Erreur déplacement fichier:", err);
    res.status(500).json({ error: "Erreur de déplacement du fichier." });
  }
});

// === Lancer le serveur ===
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Serveur lancé sur le port ${PORT}`);
});

