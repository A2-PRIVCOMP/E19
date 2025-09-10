# Questionnaire - Sensitive Interests Classification

Este repositorio contiene la estructura y archivos necesarios para la **clasificación de intereses sensibles** a través de un cuestionario web. Está diseñado para fines de investigación y ha sido adaptado para poder ser compartido de manera segura en un repositorio público.

## Estructura de directorios

- **x_questionnaire/index.html**  
  Página principal del cuestionario, donde los usuarios pueden clasificar intereses. Incluye la lógica de presentación y selección de categorías para cada interés.  
  > ⚠️ Nota: La integración real con reCAPTCHA y envío de datos a servidores se ha reemplazado por placeholders para mantener la seguridad y privacidad en el repositorio público.

- **x_questionnaire/finished.html**  
  Página que se muestra al finalizar la clasificación del cuestionario. Muestra un mensaje de agradecimiento y orientación sobre próximos pasos.

- **x_questionnaire/bot/index.html**  
  Página que se muestra cuando se detecta comportamiento sospechoso o automatizado (por ejemplo, un bot o usuario que responde demasiado rápido).  
  Utiliza un mensaje informativo para disuadir interacciones automatizadas.
- **q.css**
  Script de formato. Este script es el encargado de definir el formato visual del formulario de clasificación de intereses sensibles.
- **show_line_db.php**
  Procesamiento de Respuestas con Google reCAPTCHA v3 y MySQL. Este script en **PHP** implementa un sistema de validación de respuestas de usuarios, asegurando que la interacción sea legítima mediante **Google reCAPTCHA v3**, y almacenando la información en una base de datos MySQL.

## Características destacadas

- **Traducción automática**  
  Todas las páginas incluyen un desplegable para traducir su contenido usando **Google Translate**, permitiendo que el cuestionario sea accesible a usuarios de distintos idiomas.

- **Persistencia de usuario entre sesiones**  
  El cuestionario utiliza un identificador generado en el navegador y basado en la configuración de pantalla (fingerprinting) para permitir que un usuario pueda continuar su clasificación en diferentes sesiones, siempre que use el mismo dispositivo y navegador.

- **Clasificación de intereses sensibles**  
  Los intereses se presentan con información adicional como:  
  - Nombre del interés  
  - Categoría de desambiguación  
  - Tema relacionado  
  Los usuarios deben clasificarlos como:  
  - No sensible  
  - No conocido  
  - Sensible (Político, Religioso, Racial, Salud, Sexual, Otro)

## Características principales

- Verificación de solicitudes entrantes con:
  - Método **POST**.
  - Conexión segura (**HTTPS**).
  - Validación de `HTTP_USER_AGENT`, `HTTP_REFERER` y dominio (`<SITE>`).
- Validación de usuarios humanos vs. bots con **Google reCAPTCHA v3**.
- Manejo de credenciales de base de datos desde un archivo externo.
- Inserción y actualización de:
  - Información de panelistas (usuarios).
  - Respuestas clasificadas (`responses_to_classify`).
  - Estadísticas sobre intereses (`interests_to_classify`).
- Control del flujo de respuestas hasta completar un umbral (`4184` respuestas).
- Generación de nueva tarea para el usuario (interés a clasificar) o marcado como finalizado.

---

## Flujo del Script

1. **Validación de la petición**  
   - Acepta solo peticiones `POST` vía `HTTPS`.  
   - Verifica encabezados y dominio de origen.  

2. **Verificación reCAPTCHA v3**  
   - Envía el `token` recibido (`recaptcha_response`) a la API de Google.  
   - Si la puntuación `score >= 0.1`, se considera humano; de lo contrario, se marca como bot.  

3. **Gestión de usuarios (panelistas)**  
   - Se buscan registros existentes en `interests_database.panelists_info_to_classify`.  
   - Si no existen, se inserta un nuevo registro con metadatos (`type_user`, `country`, `gender`, `age_group`).  

4. **Almacenamiento de respuestas**  
   - Si el usuario no ha completado las `4184` respuestas:
     - Se inserta o actualiza la respuesta (`to_insert`) para un `interest_id` (`id_interest`).  
     - Se actualiza el contador correspondiente en `interests_to_classify` según la categoría de usuario (`user`, `tech`, `lawyer`).  

5. **Asignación de nuevas tareas**  
   - Se selecciona aleatoriamente un nuevo interés para clasificar, respetando límites de balance entre respuestas de distintos grupos.  
   - Si ya completó todo, se devuelve `{"finished": 1}`.  

---

## Dependencias

- **Google reCAPTCHA v3**  
  - API: `https://www.google.com/recaptcha/api/siteverify`  
  - Se requiere clave pública (`site-key`) y privada (`secret`).  

- **MySQL**  
  - Se espera un servidor MySQL accesible con credenciales almacenadas en:
    ```
    /<PATH_TO_CREDENTIALS>.txt
    ```
    en el siguiente orden:
    ```
    servidor
    usuario
    contraseña
    ```

- **PHP** con extensiones:
  - `mysqli`
  - `json`

## Uso

1. **Integrar en formulario**  
   - Incluir el script de Google reCAPTCHA v3 en el front-end:
     ```html
     <script src="https://www.google.com/recaptcha/api.js?render=<RECAPTCHA_KEY>"></script>
     ```
   - Ejecutar:
     ```js
     grecaptcha.ready(function() {
       grecaptcha.execute('RECAPTCHA_KEY', {action: 'submit'}).then(function(token) {
         document.getElementById('form').appendChild(
           Object.assign(document.createElement('input'), {
             type: 'hidden',
             name: 'recaptcha_response',
             value: token
           })
         );
       });
     });
     ```

2. **Enviar formulario vía POST** al script PHP.  

3. **El servidor validará la solicitud** y devolverá:
   - Una nueva tarea en formato JSON (`interest_id`, `name`, `topic`...).  
   - O bien `{"finished": 1}` si el usuario ha completado todas las respuestas.  
   - En caso de bot: `{"bot": 1}`.  

---

## Tablas involucradas

- `interests_database.panelists_info_to_classify` → Información del panelista.  
- `interests_database.responses_to_classify` → Respuestas de usuarios a intereses.  
- `interests_database.interests_to_classify` → Banco de intereses por clasificar, con contadores de respuestas.  

---

## Ejemplo de respuesta del servidor

```json
{
  "interest_id": "12345",
  "name": "Example Interest",
  "disambiguation": "Technology",
  "topic": "Artificial Intelligence"
}
```

## Seguridad y privacidad

Este repositorio ha sido preparado para **uso público y con fines de investigación**:

- Claves de **reCAPTCHA** y otros identificadores sensibles han sido reemplazadas por **placeholders** (`<RECAPTCHA_KEY>`).  
- No se incluyen datos de usuarios reales ni información sensible.  

> ⚠️ **Importante**: El código actual concatena variables directamente en consultas SQL, lo que puede abrir la puerta a **inyecciones SQL**. Se recomienda migrar a **sentencias preparadas**.

## Cómo usar este proyecto

1. Clonar o descargar el repositorio.
2. Abrir `index.html` en un navegador para probar la presentación del cuestionario.
3. La lógica de envío de datos a servidores está desactivada; puede implementarse un backend seguro si se desea uso real en entorno controlado.  
