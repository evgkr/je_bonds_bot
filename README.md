## Setup Guide
You will need to install <a href="https://www.python.org/downloads/">Python</a> to run this bot. This setup guide is for windows users since Linux users will probably be able to figure things out on their own.

### 1. Open command line using Win + R hotkey

### 2. Type in <i>cmd</i> and press Enter

### 3. Clone the repository
```bash
git clone https://github.com/evgkr/je_bonds_bot.git
```

### 4. Navigate to the project directory
```bash
cd je_bonds_bot
```

### 5. Install <i>virtualenv</i> package
```bash
pip install virtualenv
```

### 6. Create & activate <i>virtualenv</i>
```bash
virtualenv myenv
```

For Windows 10:
```bash
cd myenv/Scripts
```
```bash
activate
```

For Windows 11:
```bash
myenv/Scripts/activate
```

### 7. Install project dependencies
```bash
pip install -r requirements.txt
```

### 8. Create .env file
```bash
copy tokenAPI .env
```

### 9. Open .env file and paste your tinkoff & telegram tokens

### 10. Start the app
```bash
python bonds_project_main.py
```
