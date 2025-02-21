
import pygame
import sys
import math
import random
import time
import csv
from pyvirtualdisplay import Display

# Start a virtual display
display = Display(visible=0, size=(800, 600))
display.start()

# Initialize Pygame
pygame.init()

# Screen dimensions
WIDTH, HEIGHT = 600, 600
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Pong Circle - Simplified")

# Colors
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
RED = (255, 0, 0)
GREEN = (0, 255, 0)

# Game variables
CENTER = (WIDTH // 2, HEIGHT // 2)
RADIUS = 200
BALL_RADIUS = 10
PADDLE_LENGTH = 80
PADDLE_WIDTH = 10

# Font
font = pygame.font.SysFont(None, 36)

# Player details
player_name = input("Enter Player Name: ")
player_age = input("Enter Player Age: ")

# Ball setup
ball_angle = random.uniform(0, 2 * math.pi)
ball_speed = 3
ball_dx = ball_speed * math.cos(ball_angle)
ball_dy = ball_speed * math.sin(ball_angle)
ball_pos = [CENTER[0] + RADIUS * math.cos(ball_angle), CENTER[1] + RADIUS * math.sin(ball_angle)]

# Paddle setup
paddle_angle = 0
paddle_speed = 0.05

# Tracking variables
hits = 0
fastest_speed = 0
game_start_time = time.time()

# Functions
def draw_circle():
    pygame.draw.circle(screen, WHITE, CENTER, RADIUS, 2)

def draw_ball(pos):
    pygame.draw.circle(screen, RED, (int(pos[0]), int(pos[1])), BALL_RADIUS)

def draw_paddle(angle):
    x1 = CENTER[0] + (RADIUS - 10) * math.cos(angle)
    y1 = CENTER[1] + (RADIUS - 10) * math.sin(angle)
    x2 = CENTER[0] + (RADIUS + 10) * math.cos(angle)
    y2 = CENTER[1] + (RADIUS + 10) * math.sin(angle)
    pygame.draw.line(screen, GREEN, (x1, y1), (x2, y2), PADDLE_WIDTH)

def display_stats(hits, fastest_speed, elapsed_time):
    hits_text = font.render(f"Hits: {hits}", True, WHITE)
    speed_text = font.render(f"Fastest Speed: {fastest_speed:.2f}", True, WHITE)
    time_text = font.render(f"Time: {int(elapsed_time)}s", True, WHITE)
    screen.blit(hits_text, (20, 20))
    screen.blit(speed_text, (20, 60))
    screen.blit(time_text, (20, 100))

def export_results():
    elapsed_time = time.time() - game_start_time
    with open('game_results.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([player_age, player_name, hits, round(fastest_speed, 2), int(elapsed_time)])

# Game loop
clock = pygame.time.Clock()
running = True

while running:
    screen.fill(BLACK)
    elapsed_time = time.time() - game_start_time

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # Paddle control
    keys = pygame.key.get_pressed()
    if keys[pygame.K_LEFT]:
        paddle_angle -= paddle_speed
    if keys[pygame.K_RIGHT]:
        paddle_angle += paddle_speed

    # Update ball position
    ball_pos[0] += ball_dx
    ball_pos[1] += ball_dy

    # Check for collision with circle boundary
    dist_to_center = math.hypot(ball_pos[0] - CENTER[0], ball_pos[1] - CENTER[1])
    if dist_to_center >= RADIUS - BALL_RADIUS:
        angle_to_center = math.atan2(ball_pos[1] - CENTER[1], ball_pos[0] - CENTER[0])
        ball_dx = -ball_dx
        ball_dy = -ball_dy

        # Check for paddle hit
        paddle_diff = abs(angle_to_center - paddle_angle) % (2 * math.pi)
        if paddle_diff < 0.2 or paddle_diff > (2 * math.pi - 0.2):
            hits += 1
            ball_speed *= 1.05
            fastest_speed = max(fastest_speed, ball_speed)
            ball_dx = ball_speed * math.cos(angle_to_center)
            ball_dy = ball_speed * math.sin(angle_to_center)

    # Draw game elements
    draw_circle()
    draw_ball(ball_pos)
    draw_paddle(paddle_angle)
    display_stats(hits, fastest_speed, elapsed_time)

    pygame.display.flip()
    clock.tick(60)

# Export results and quit
export_results()
pygame.quit()
sys.exit()
