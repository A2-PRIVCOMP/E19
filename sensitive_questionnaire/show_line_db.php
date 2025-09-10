<?php
header ('Content-type: text/html; charset=utf-8');
if($_SERVER['REQUEST_METHOD'] === 'POST' && $_SERVER['HTTPS'] == "on" && $_SERVER['HTTP_USER_AGENT'] && $_SERVER['HTTP_REFERER'] && ($_SERVER['HTTP_HOST'] == "www.<SITE>" | $_SERVER['HTTP_HOST'] == "<SITE>")){ 
    // Realizamos la petición de control: 
    $recaptcha_url = 'https://www.google.com/recaptcha/api/siteverify'; 
    $recaptcha_secret = '<RECAPTCHA_SECRET>'; 
    $recaptcha_response = $_POST['recaptcha_response']; 
    $recaptcha = file_get_contents($recaptcha_url . '?secret=' . $recaptcha_secret . '&response=' . $recaptcha_response); 
    $recaptcha = json_decode($recaptcha);
    // Miramos si se considera humano o robot:
    echo $recaptcha->score;
    if($recaptcha->score >= 0.1){
        //We read the database credentials
        $credentials = array();
        $auth = @fopen("/<PATH_TO_CREDENTIALS>.txt", "r");
        if ($auth) {
            while (($buffer = fgets($auth, 4096)) !== false) {
                array_push($credentials, $buffer);
            }
            if (!feof($auth)) {
                echo "Error: unexpected fgets() fail\n";
            }
            fclose($auth);
        }

        $servername = trim($credentials[0]);
        $username = trim($credentials[1]);
        $password = trim($credentials[2]);

        $to_insert = $_POST['to_insert'];
        $id_interest = $_POST['id_interest'];
        $type_user = $_POST['u_cat'];
        $user = $_POST['fingerprint'];
        $country = $_POST['country'];
        $gender = $_POST['gender'];
        $age_group = $_POST['age_group'];
        $n_responses_group = 5;

        if (substr( $type_user, 0, 4 ) === "USER"){
            $category = 'user_responses';
        } else if (substr( $type_user, 0, 4 ) === "TECH") {
            $category = 'tech_responses';
        } else {
            $category = 'lawyer_responses';
        }
        // Create connection
        $conn = new mysqli($servername, $username, $password);

        // Check connection
        if ($conn->connect_error) {
            die("Connection failed: " . $conn->connect_error);
        }
        mysqli_set_charset($conn,"utf8");
        $obtencion_user = "select id from interests_database.panelists_info_to_classify where user = '".$user."' and type_user = '".$type_user."' and country = '".$country."' and gender = ".$gender." and age_group = ".$age_group;
        $result = $conn->query($obtencion_user);
        if ($result->num_rows >= 1) {
            while($row = $result->fetch_assoc()) {
                $user = $row['id'];
            }
        }else{
            $sql = "INSERT INTO interests_database.panelists_info_to_classify (timestamp, user, type_user, country, gender, age_group)
                VALUES (now(),'".$user."','".$type_user."','".$country."','".$gender."','".$age_group."')";

            if ($conn->query($sql) === TRUE) {
                echo "New record created successfully";
            } else {
                echo "Error: " . $sql . "<br>" . $conn->error;
            }
        }
        $check_user_finished = "SELECT * from interests_database.responses_to_classify where user = ".$user." group by interest_id";
        $result = $conn->query($check_user_finished);
        if ($result->num_rows < 4184) {
            if (strlen($id_interest) > 1){
                $check_response_inserted = "select * from interests_database.responses_to_classify where interest_id = '".$id_interest."' and user = ".$user.";";
                $r2 = $conn->query($check_response_inserted);
                if($r2->num_rows < 1){
                    $sql = "INSERT INTO interests_database.responses_to_classify (timestamp, interest_id, user, response)
                    VALUES (now(),'".$id_interest."','".$user."','".$to_insert."')";

                    if ($conn->query($sql) === TRUE) {
                        echo "New record created successfully";
                        echo substr( $type_user, 0, 4 );
                        $update_interest = "UPDATE interests_database.interests_to_classify SET ".$category." = ".$category." + 1 WHERE interest_id = '".$id_interest."';";
                        $r3 = $conn->query($update_interest);
                    } else {
                        echo "Error: " . $sql . "<br>" . $conn->error;
                    }
                }else{
                    $sql = "UPDATE interests_database.responses_to_classify set timestamp = now(), response = '".$to_insert."' where interest_id = '".$id_interest."' and user = ".$user.";";

                    if ($conn->query($sql) === TRUE) {
                        echo "Record corrected successfully";
                    } else {
                        echo "Error: " . $sql . "<br>" . $conn->error;
                    }
                }
            }
        }
        $check_user_finished = "SELECT * from interests_database.responses_to_classify where user = ".$user." group by interest_id";
        $result = $conn->query($check_user_finished);
        if ($result->num_rows < 4184) {
            do{
                //Query to show next row
                $query = "select ii.interest_id, name, ii.disambiguation, ii.topic from interests_database.interests_to_classify ii
                inner join (
                    select interest_id from (
                    select i.interest_id from interests_database.interests_to_classify i 
                    union all
                    select r.interest_id from interests_database.responses_to_classify r where user = ".$user.") t
                    GROUP BY interest_id 
                    HAVING COUNT(*) = 1 ) tt
                on tt.interest_id = ii.interest_id and ".$category." < ".$n_responses_group." 
                order by block_number, ".$category.", rand() limit 1;";
                $result = $conn->query($query);
                if ($result->num_rows > 0) {
                    // output data of each row
                    while($row = $result->fetch_assoc()) {
                        //echo $row['interest_id'] . $row['name'] . $row['disambiguation'] . $row['topic'];
                        echo json_encode($row, JSON_UNESCAPED_UNICODE);
                    }
                    break;
                }else{
                    $n_responses_group = $n_responses_group + 5;
                }
            }while(0);
        } else {
            echo "{\"finished\": 1}";
        }
        $conn->close();
    }else if(isset($recaptcha->score)){
        echo $recaptcha->score;
        echo "{\"bot\": 1}";
    }
}else echo"<script>window.location.href=\"..\"</script>";
?>