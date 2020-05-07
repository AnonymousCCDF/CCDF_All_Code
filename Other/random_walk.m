% Demo to do a random walk in 2 dimensions.
% User is asked for the number of steps to take.
% By Image Analyst
clc; % Clear the command window.
clearvars;
close all; % Close all figures (except those of imtool.)
workspace; % Make sure the workspace panel is showing.
fontSize = 20;
format compact;

% Ask user for a number of steps to take.
defaultValue = 20;
titleBar = 'Enter an integer value';
userPrompt = 'Enter the number of steps to take: ';
caUserInput = inputdlg(userPrompt, userPrompt, 1, {num2str(defaultValue)});
if isempty(caUserInput),return,end; % Bail out if they clicked Cancel.
integerValue = round(str2num(cell2mat(caUserInput)));

numberOfSteps = integerValue;

% early_shuffle = rand(numberOfSteps);
% reducer = rand(numberOfSteps) * 4; 

executor_num = rand(numberOfSteps) * 4;
executor_memory = rand(numberOfSteps) * 4;
executor_cpu = rand(numberOfSteps) * 4;

yarn_am = rand(numberOfSteps) * 0.75 + 0.25; 
yarn_node_locality = rand(numberOfSteps) * 10;

dfs_block = rand(numberOfSteps) * 4 + 5; 
dfs_replication = rand(numberOfSteps) * 5 ; 
xy = zeros(numberOfSteps,7);


for step = 1 : numberOfSteps
% Walk in the xy direction.
% xy(step, 1) = xy(step, 1) + early_shuffle(step);
% xy(step, 2) = xy(step, 2) + reducer(step);

xy(step, 1) = xy(step, 1) + executor_num(step);
xy(step, 2) = xy(step, 2) + executor_memory(step);
xy(step, 3) = xy(step, 3) + executor_cpu(step);

xy(step, 4) = xy(step,4) + yarn_am(step);
xy(step, 5) = xy(step,5) + yarn_node_locality(step);
xy(step, 6) = xy(step,6) + dfs_block(step);
xy(step, 7) = xy(step,7) + dfs_replication(step);
% Now plot the walk so far.
end




