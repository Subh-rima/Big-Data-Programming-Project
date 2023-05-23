# big-data-programming-april2022-team-maas
big-data-programming-april2022-team-maas created by GitHub Classroom

Introduction:
Doodle, a famous software company wishes to automate their recruiting process and build an in-house solution that selects potential candidate.
Hush Hush Recruiter is the Candidate Selection Algorithm that automates the process of selecting Potential Candidate from Pool of Candidate using data from Github and Stack Overflow, and sends email to the Candidate, after clustering the Potential Candidate who has been selected after applying the Algorithm.

Candidate Data Source:
1.Github: Using pyGithub Module in Python extracted Github User Data
2.Stack Overflow: 
a)  https://api.stackexchange.com/2.2/questions?order=desc&sort=activity&site=stackoverflow&pagesize=100&page={1}
b) https://api.stackexchange.com/2.3/users/{x}/top-tags?pagesize=1&site=stackoverflow

How our Selection Algorithm Works:
 At first, User Data is extracted from Github and Stack Overflow and stored in MySQL. Data is combined based on the username using fuzzy logic. Then Standard Scaler was implemented to standardize the Features before implementing K-Means Clustering and Logistic Regression.
With K-Means two clusters ‘Selected’ and ‘Not Selected’ are formed. The potential candidates are clustered.
The Selected Candidates are stored in MongoDB. Email is being sent to the Selected Candidates by the Candidate Details stored in MongoDB. Emails are sent to the selected Candidate with the Interface having 3 Coding Question. The responses of the coding questions will be sent back to the admin mail id.
Classifying candidates to selected and not selected achieved high accuracy of 99 percent by using Logistic Regression. This model can be modified further to classify new candidates.

Future Scope in our Project:
1.Selection of Top 5 Candidates
2.Having Interface for the Recruiting Manager

Files:
db_scripts.py: Contains the database scripts for MySQL. 
Main.py: Preprocessing and algorithm applied on combined data and email sent.
Merge_git_stack.py: GitHub and Stack combined using fuzzy logic
github_algorithm.py: Preprocessing and algorithm applied on github data and email sent.
Stack_algorithm.py: Preprocessing and algorithm applied on stack data and email sent.

Application Interface:
https://forms.pabbly.com/form/share/o9ys-493000 

Coding Questions: 
 https://docs.google.com/forms/d/e/1FAIpQLSfDfH_iL3hQSrtizIEmc5nyEjUd3Gz9MjGX1IuK-wKpe1StoA/viewform?usp=sf_link
